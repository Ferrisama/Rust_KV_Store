// src/cluster/mod.rs

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use log::info;

#[derive(Clone)]
pub struct ClusterNode {
    pub id: String,
    pub address: String,
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

    pub async fn get_node_for_key(&self, key: &str) -> Option<(ClusterNode, ClusterNode)> {
        let nodes = self.nodes.read().await;
        let virtual_nodes = self.virtual_nodes.read().await;

        if nodes.is_empty() {
            return None;
        }

        let hash = self.hash(key);
        let mut primary_virtual_node = u64::MAX;
        let mut backup_virtual_node = u64::MAX;

        for &vn in virtual_nodes.keys() {
            if vn > hash && vn < primary_virtual_node {
                backup_virtual_node = primary_virtual_node;
                primary_virtual_node = vn;
            } else if vn < backup_virtual_node {
                backup_virtual_node = vn;
            }
        }

        if primary_virtual_node == u64::MAX {
            primary_virtual_node = *virtual_nodes.keys().min().unwrap();
        }
        if backup_virtual_node == u64::MAX {
            backup_virtual_node = *virtual_nodes.keys().min().unwrap();
        }

        let primary_node_id = virtual_nodes.get(&primary_virtual_node).unwrap();
        let backup_node_id = virtual_nodes.get(&backup_virtual_node).unwrap();

        let primary_node = nodes.iter().find(|node| &node.id == primary_node_id).cloned()?;
        let backup_node = nodes.iter().find(|node| &node.id == backup_node_id).cloned()?;

        Some((primary_node, backup_node))
    }

    fn hash(&self, key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}