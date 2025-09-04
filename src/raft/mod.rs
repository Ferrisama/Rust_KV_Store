// src/raft/mod.rs

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use log::{info, warn, error, debug};
use rand::Rng;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    Set { key: String, value: String },
    Delete { key: String },
    NoOp, // Used for heartbeats
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
}

pub struct RaftNode {
    // Persistent state
    pub id: String,
    pub peers: Vec<String>,
    current_term: Arc<Mutex<u64>>,
    voted_for: Arc<Mutex<Option<String>>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
    
    // Volatile state
    pub state: Arc<RwLock<NodeState>>,
    commit_index: Arc<Mutex<u64>>,
    last_applied: Arc<Mutex<u64>>,
    
    // Leader state (reinitialized after election)
    next_index: Arc<Mutex<HashMap<String, u64>>>,
    match_index: Arc<Mutex<HashMap<String, u64>>>,
    
    // Application state
    state_machine: Arc<Mutex<HashMap<String, String>>>,
    
    // Timing
    last_heartbeat: Arc<Mutex<Instant>>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
}

impl RaftNode {
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let election_timeout = Duration::from_millis(150 + rand::thread_rng().gen_range(0..150));
        
        Self {
            id,
            peers,
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(vec![LogEntry {
                term: 0,
                index: 0,
                command: RaftCommand::NoOp,
            }])), // Start with dummy entry
            state: Arc::new(RwLock::new(NodeState::Follower)),
            commit_index: Arc::new(Mutex::new(0)),
            last_applied: Arc::new(Mutex::new(0)),
            next_index: Arc::new(Mutex::new(HashMap::new())),
            match_index: Arc::new(Mutex::new(HashMap::new())),
            state_machine: Arc::new(Mutex::new(HashMap::new())),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            election_timeout,
            heartbeat_interval: Duration::from_millis(50),
        }
    }
    
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting Raft node {}", self.id);
        
        // Start election timeout checker
        let election_checker = self.clone();
        tokio::spawn(async move {
            election_checker.election_timeout_loop().await;
        });
        
        // Start heartbeat sender (if leader)
        let heartbeat_sender = self.clone();
        tokio::spawn(async move {
            heartbeat_sender.heartbeat_loop().await;
        });
        
        // Start log applier
        let log_applier = self.clone();
        tokio::spawn(async move {
            log_applier.apply_logs_loop().await;
        });
        
        Ok(())
    }
    
    async fn election_timeout_loop(&self) {
        let mut interval = interval(Duration::from_millis(50));
        
        loop {
            interval.tick().await;
            
            if matches!(*self.state.read().await, NodeState::Leader) {
                continue;
            }
            
            let last_heartbeat = *self.last_heartbeat.lock().await;
            if last_heartbeat.elapsed() > self.election_timeout {
                info!("Election timeout for node {}, starting election", self.id);
                if let Err(e) = self.start_election().await {
                    error!("Failed to start election: {}", e);
                }
            }
        }
    }
    
    async fn heartbeat_loop(&self) {
        let mut interval = interval(self.heartbeat_interval);
        
        loop {
            interval.tick().await;
            
            if matches!(*self.state.read().await, NodeState::Leader) {
                if let Err(e) = self.send_heartbeats().await {
                    error!("Failed to send heartbeats: {}", e);
                }
            }
        }
    }
    
    async fn apply_logs_loop(&self) {
        let mut interval = interval(Duration::from_millis(10));
        
        loop {
            interval.tick().await;
            
            let commit_index = *self.commit_index.lock().await;
            let mut last_applied = self.last_applied.lock().await;
            
            if commit_index > *last_applied {
                let log = self.log.lock().await;
                for i in (*last_applied + 1)..=commit_index {
                    if let Some(entry) = log.iter().find(|e| e.index == i) {
                        self.apply_to_state_machine(&entry.command).await;
                        *last_applied = i;
                        debug!("Applied log entry {} to state machine", i);
                    }
                }
            }
        }
    }
    
    async fn apply_to_state_machine(&self, command: &RaftCommand) {
        let mut state_machine = self.state_machine.lock().await;
        
        match command {
            RaftCommand::Set { key, value } => {
                state_machine.insert(key.clone(), value.clone());
                info!("Applied SET {} = {}", key, value);
            }
            RaftCommand::Delete { key } => {
                state_machine.remove(key);
                info!("Applied DELETE {}", key);
            }
            RaftCommand::NoOp => {
                // No-op, used for heartbeats
            }
        }
    }
    
    async fn start_election(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Transition to candidate
        *self.state.write().await = NodeState::Candidate;
        
        // Increment term and vote for self
        let mut current_term = self.current_term.lock().await;
        *current_term += 1;
        let term = *current_term;
        drop(current_term);
        
        *self.voted_for.lock().await = Some(self.id.clone());
        
        info!("Node {} starting election for term {}", self.id, term);
        
        // Get last log info
        let log = self.log.lock().await;
        let last_log_index = log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);
        drop(log);
        
        // Send RequestVote to all peers
        let mut votes = 1; // Vote for self
        let needed_votes = (self.peers.len() + 1) / 2 + 1;
        
        let mut tasks = Vec::new();
        
        for peer in &self.peers {
            let peer = peer.clone();
            let id = self.id.clone();
            let request = RequestVoteRequest {
                term,
                candidate_id: id,
                last_log_index,
                last_log_term,
            };
            
            let task = tokio::spawn(async move {
                Self::send_request_vote(&peer, request).await
            });
            tasks.push(task);
        }
        
        // Wait for responses
        for task in tasks {
            if let Ok(Ok(response)) = task.await {
                if response.term > term {
                    // Higher term discovered, step down
                    *self.current_term.lock().await = response.term;
                    *self.voted_for.lock().await = None;
                    *self.state.write().await = NodeState::Follower;
                    return Ok(());
                }
                
                if response.vote_granted {
                    votes += 1;
                    if votes >= needed_votes {
                        // Won election!
                        return self.become_leader(term).await;
                    }
                }
            }
        }
        
        // Didn't win election, become follower
        *self.state.write().await = NodeState::Follower;
        info!("Node {} lost election for term {}", self.id, term);
        
        Ok(())
    }
    
    async fn become_leader(&self, term: u64) -> Result<(), Box<dyn std::error::Error>> {
        info!("Node {} became leader for term {}", self.id, term);
        
        *self.state.write().await = NodeState::Leader;
        
        // Initialize leader state
        let mut next_index = self.next_index.lock().await;
        let mut match_index = self.match_index.lock().await;
        
        let last_log_index = {
            let log = self.log.lock().await;
            log.last().map(|e| e.index).unwrap_or(0)
        };
        
        for peer in &self.peers {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }
        
        // Send initial heartbeat to establish authority
        self.send_heartbeats().await?;
        
        Ok(())
    }
    
    async fn send_heartbeats(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_term = *self.current_term.lock().await;
        let commit_index = *self.commit_index.lock().await;
        
        for peer in &self.peers {
            let next_index = self.next_index.lock().await;
            let prev_log_index = next_index.get(peer).copied().unwrap_or(1) - 1;
            drop(next_index);
            
            let (prev_log_term, entries) = {
                let log = self.log.lock().await;
                let prev_log_term = log.iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0);
                
                // For heartbeat, send empty entries
                (prev_log_term, Vec::new())
            };
            
            let request = AppendEntriesRequest {
                term: current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };
            
            let peer_clone = peer.clone();
            let node_id = self.id.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::send_append_entries(&peer_clone, request).await {
                    debug!("Failed to send heartbeat to {}: {}", peer_clone, e);
                }
            });
        }
        
        Ok(())
    }
    
    pub async fn propose_command(&self, command: RaftCommand) -> Result<(), Box<dyn std::error::Error>> {
        if !matches!(*self.state.read().await, NodeState::Leader) {
            return Err("Not the leader".into());
        }
        
        let current_term = *self.current_term.lock().await;
        
        // Add entry to log
        let new_index = {
            let mut log = self.log.lock().await;
            let new_index = log.last().map(|e| e.index).unwrap_or(0) + 1;
            log.push(LogEntry {
                term: current_term,
                index: new_index,
                command: command.clone(),
            });
            new_index
        };
        
        info!("Leader {} proposed command at index {}", self.id, new_index);
        
        // Replicate to followers
        self.replicate_entries().await?;
        
        Ok(())
    }
    
    async fn replicate_entries(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_term = *self.current_term.lock().await;
        let commit_index = *self.commit_index.lock().await;
        
        for peer in &self.peers {
            let (prev_log_index, prev_log_term, entries) = {
                let next_index = self.next_index.lock().await;
                let next_idx = next_index.get(peer).copied().unwrap_or(1);
                let prev_log_index = next_idx - 1;
                drop(next_index);
                
                let log = self.log.lock().await;
                let prev_log_term = log.iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0);
                
                let entries: Vec<LogEntry> = log.iter()
                    .filter(|e| e.index >= next_idx)
                    .cloned()
                    .collect();
                
                (prev_log_index, prev_log_term, entries)
            };
            
            let request = AppendEntriesRequest {
                term: current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };
            
            // Send and handle response
            let peer_clone = peer.clone();
            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Ok(response) = Self::send_append_entries(&peer_clone, request).await {
                    self_clone.handle_append_entries_response(&peer_clone, response).await;
                }
            });
        }
        
        Ok(())
    }
    
    async fn handle_append_entries_response(&self, peer: &str, response: AppendEntriesResponse) {
        if response.term > *self.current_term.lock().await {
            // Step down
            *self.current_term.lock().await = response.term;
            *self.voted_for.lock().await = None;
            *self.state.write().await = NodeState::Follower;
            return;
        }
        
        if response.success {
            if let Some(match_index) = response.match_index {
                self.match_index.lock().await.insert(peer.to_string(), match_index);
                self.next_index.lock().await.insert(peer.to_string(), match_index + 1);
                
                // Update commit index if majority agrees
                self.update_commit_index().await;
            }
        } else {
            // Decrement next_index and retry
            let mut next_index = self.next_index.lock().await;
            if let Some(idx) = next_index.get_mut(peer) {
                if *idx > 1 {
                    *idx -= 1;
                }
            }
        }
    }
    
    async fn update_commit_index(&self) {
        let match_indices: Vec<u64> = {
            let match_index = self.match_index.lock().await;
            let mut indices: Vec<u64> = match_index.values().cloned().collect();
            
            // Add leader's match index (latest log entry)
            let log = self.log.lock().await;
            if let Some(last_entry) = log.last() {
                indices.push(last_entry.index);
            }
            indices
        };
        
        if !match_indices.is_empty() {
            let mut sorted_indices = match_indices;
            sorted_indices.sort_by(|a, b| b.cmp(a)); // Sort descending
            
            let majority_index = (sorted_indices.len() - 1) / 2;
            let new_commit_index = sorted_indices[majority_index];
            
            let current_commit = *self.commit_index.lock().await;
            if new_commit_index > current_commit {
                // Verify the entry is from current term
                let log = self.log.lock().await;
                if let Some(entry) = log.iter().find(|e| e.index == new_commit_index) {
                    if entry.term == *self.current_term.lock().await {
                        *self.commit_index.lock().await = new_commit_index;
                        info!("Updated commit index to {}", new_commit_index);
                    }
                }
            }
        }
    }
    
    pub async fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = *self.current_term.lock().await;
        
        // Update last heartbeat
        *self.last_heartbeat.lock().await = Instant::now();
        
        // Reply false if term < currentTerm
        if request.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: None,
            };
        }
        
        // If term > currentTerm, update term and step down
        if request.term > current_term {
            *self.current_term.lock().await = request.term;
            *self.voted_for.lock().await = None;
            *self.state.write().await = NodeState::Follower;
        }
        
        // Check log consistency
        let log_consistent = {
            let log = self.log.lock().await;
            if request.prev_log_index == 0 {
                true
            } else {
                log.iter().any(|e| e.index == request.prev_log_index && e.term == request.prev_log_term)
            }
        };
        
        if !log_consistent {
            return AppendEntriesResponse {
                term: request.term,
                success: false,
                match_index: None,
            };
        }
        
        // Append new entries
        if !request.entries.is_empty() {
            let mut log = self.log.lock().await;
            
            // Remove conflicting entries
            log.retain(|e| e.index <= request.prev_log_index);
            
            // Append new entries
            for entry in &request.entries {
                log.push(entry.clone());
            }
        }
        
        // Update commit index
        if request.leader_commit > *self.commit_index.lock().await {
            let last_new_index = request.entries.last()
                .map(|e| e.index)
                .unwrap_or(request.prev_log_index);
            
            let new_commit = std::cmp::min(request.leader_commit, last_new_index);
            *self.commit_index.lock().await = new_commit;
        }
        
        let match_index = if request.entries.is_empty() {
            None
        } else {
            request.entries.last().map(|e| e.index)
        };
        
        AppendEntriesResponse {
            term: request.term,
            success: true,
            match_index,
        }
    }
    
    pub async fn handle_request_vote(&self, request: RequestVoteRequest) -> RequestVoteResponse {
        let current_term = *self.current_term.lock().await;
        
        // Reply false if term < currentTerm
        if request.term < current_term {
            return RequestVoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }
        
        // If term > currentTerm, update term and step down
        if request.term > current_term {
            *self.current_term.lock().await = request.term;
            *self.voted_for.lock().await = None;
            *self.state.write().await = NodeState::Follower;
        }
        
        let voted_for = self.voted_for.lock().await.clone();
        
        // Check if we can vote for this candidate
        let can_vote = voted_for.is_none() || voted_for.as_ref() == Some(&request.candidate_id);
        
        if can_vote {
            // Check if candidate's log is at least as up-to-date as ours
            let log = self.log.lock().await;
            let our_last_term = log.last().map(|e| e.term).unwrap_or(0);
            let our_last_index = log.last().map(|e| e.index).unwrap_or(0);
            
            let candidate_up_to_date = request.last_log_term > our_last_term ||
                (request.last_log_term == our_last_term && request.last_log_index >= our_last_index);
            
            if candidate_up_to_date {
                *self.voted_for.lock().await = Some(request.candidate_id.clone());
                info!("Node {} voted for {} in term {}", self.id, request.candidate_id, request.term);
                
                return RequestVoteResponse {
                    term: request.term,
                    vote_granted: true,
                };
            }
        }
        
        RequestVoteResponse {
            term: request.term,
            vote_granted: false,
        }
    }
    
    pub async fn get(&self, key: &str) -> Option<String> {
        let state_machine = self.state_machine.lock().await;
        state_machine.get(key).cloned()
    }
    
    pub async fn list_keys(&self) -> Vec<String> {
        let state_machine = self.state_machine.lock().await;
        state_machine.keys().cloned().collect()
    }
    
    pub async fn is_leader(&self) -> bool {
        matches!(*self.state.read().await, NodeState::Leader)
    }
    
    async fn send_append_entries(
        address: &str, 
        request: AppendEntriesRequest
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(address).await?;
        
        let message = RaftMessage::AppendEntries(request);
        let serialized = serde_json::to_string(&message)?;
        
        stream.write_all(serialized.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        
        let mut buffer = [0; 4096];
        let n = stream.read(&mut buffer).await?;
        let response_str = String::from_utf8_lossy(&buffer[..n]);
        
        if let RaftMessage::AppendEntriesResponse(response) = serde_json::from_str(&response_str)? {
            Ok(response)
        } else {
            Err("Invalid response type".into())
        }
    }
    
    async fn send_request_vote(
        address: &str, 
        request: RequestVoteRequest
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(address).await?;
        
        let message = RaftMessage::RequestVote(request);
        let serialized = serde_json::to_string(&message)?;
        
        stream.write_all(serialized.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        
        let mut buffer = [0; 4096];
        let n = stream.read(&mut buffer).await?;
        let response_str = String::from_utf8_lossy(&buffer[..n]);
        
        if let RaftMessage::RequestVoteResponse(response) = serde_json::from_str(&response_str)? {
            Ok(response)
        } else {
            Err("Invalid response type".into())
        }
    }
}

impl Clone for RaftNode {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            peers: self.peers.clone(),
            current_term: Arc::clone(&self.current_term),
            voted_for: Arc::clone(&self.voted_for),
            log: Arc::clone(&self.log),
            state: Arc::clone(&self.state),
            commit_index: Arc::clone(&self.commit_index),
            last_applied: Arc::clone(&self.last_applied),
            next_index: Arc::clone(&self.next_index),
            match_index: Arc::clone(&self.match_index),
            state_machine: Arc::clone(&self.state_machine),
            last_heartbeat: Arc::clone(&self.last_heartbeat),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
        }
    }
}