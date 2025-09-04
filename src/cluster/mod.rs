// src/cluster/mod.rs

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use log::info;

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: String,
    pub address: String,
}

#[derive(Serialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub virtual_nodes_per_node: usize,
    pub total_virtual_nodes: usize,
}

pub struct Cluster {
    nodes: Arc<RwLock<Vec<ClusterNode>>>,
    virtual_nodes: Arc<RwLock<HashMap<u64, String>>>,
    virtual_node_count: usize,
}

impl Cluster {
    pub fn new(virtual_node_count: usize) -> Self {
        Cluster {
            nodes: Arc::new(RwLock::new(Vec::new())),
            virtual_nodes: Arc::new(RwLock::new(HashMap::new())),
            virtual_node_count,
        }
    }

    pub async fn add_node(&self, node: ClusterNode) {
        info!("Adding node {} at {}", node.id, node.address);
        let mut nodes = self.nodes.write().await;
        let mut virtual_nodes = self.virtual_nodes.write().await;

        nodes.push(node.clone());

        for i in 0..self.virtual_node_count {
            let key = self.hash(&format!("{}:{}", node.id, i));
            virtual_nodes.insert(key, node.id.clone());
        }
    }

    pub async fn remove_node(&self, node_id: &str) {
        info!("Removing node {}", node_id);
        let mut nodes = self.nodes.write().await;
        let mut virtual_nodes = self.virtual_nodes.write().await;

        nodes.retain(|node| node.id != node_id);
        virtual_nodes.retain(|_, id| id != node_id);
    }

    pub async fn get_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().await.clone()
    }

    pub async fn get_stats(&self) -> ClusterStats {
        let nodes = self.nodes.read().await;
        let virtual_nodes = self.virtual_nodes.read().await;
        
        ClusterStats {
            total_nodes: nodes.len(),
            virtual_nodes_per_node: self.virtual_node_count,
            total_virtual_nodes: virtual_nodes.len(),
        }
    }

    pub async fn get_node_for_key(&self, key: &str) -> Option<(ClusterNode, ClusterNode)> {
        let nodes = self.nodes.read().await;
        let virtual_nodes = self.virtual_nodes.read().await;

        if nodes.is_empty() {
            return None;
        }

        let hash = self.hash(key);
        let mut sorted_vnodes: Vec<u64> = virtual_nodes.keys().cloned().collect();
        sorted_vnodes.sort();

        // Find the first virtual node that comes after our hash
        let primary_vnode = sorted_vnodes
            .iter()
            .find(|&&vn| vn > hash)
            .cloned()
            .unwrap_or(sorted_vnodes[0]);

        // Find the next virtual node for backup
        let primary_idx = sorted_vnodes.iter().position(|&vn| vn == primary_vnode).unwrap();
        let backup_vnode = sorted_vnodes[(primary_idx + 1) % sorted_vnodes.len()];

        let primary_node_id = virtual_nodes.get(&primary_vnode)?;
        let backup_node_id = virtual_nodes.get(&backup_vnode)?;

        let primary_node = nodes.iter().find(|node| &node.id == primary_node_id).cloned()?;
        let backup_node = nodes.iter().find(|node| &node.id == backup_node_id).cloned()?;

        Some((primary_node, backup_node))
    }

    pub async fn get_key_distribution(&self) -> Vec<(String, String)> {
        let virtual_nodes = self.virtual_nodes.read().await;
        let mut distribution: Vec<(String, String)> = Vec::new();
        
        for (hash, node_id) in virtual_nodes.iter() {
            distribution.push((format!("{:016x}", hash), node_id.clone()));
        }
        
        distribution.sort_by_key(|(hash, _)| hash.clone());
        distribution
    }

    fn hash(&self, key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}