// src/node/mod.rs

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use log::{info, error};
use crate::cluster::Cluster;

pub struct Node {
    id: String,
    data: Arc<Mutex<HashMap<String, String>>>,
    address: String,
    cluster: Arc<Cluster>,
}

impl Node {
    pub fn new(id: String, address: String, cluster: Arc<Cluster>) -> Self {
        Node {
            id,
            data: Arc::new(Mutex::new(HashMap::new())),
            address,
            cluster,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.address).await?;
        info!("Node {} listening on {}", self.id, self.address);

        loop {
            let (socket, _) = listener.accept().await?;
            let node = self.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, node).await {
                    error!("Error handling connection: {}", e);
                }
            });
        }
    }

    async fn replicate_to_backup(&self, key: &str, value: &str, backup_address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(backup_address).await?;
        let command = format!("REPLICATE {} {}", key, value);
        stream.write_all(command.as_bytes()).await?;

        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await?;
        let response = String::from_utf8_lossy(&buffer[..n]);

        if response != "OK" {
            return Err(format!("Replication failed: {}", response).into());
        }

        Ok(())
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            id: self.id.clone(),
            data: Arc::clone(&self.data),
            address: self.address.clone(),
            cluster: Arc::clone(&self.cluster),
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    node: Node,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0; 1024];

    loop {
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }

        let request = String::from_utf8_lossy(&buffer[..n]);
        let parts: Vec<&str> = request.split_whitespace().collect();

        if parts.is_empty() {
            continue;
        }

        let response = match parts[0] {
            "GET" => {
                if parts.len() != 2 {
                    "ERROR: Invalid GET command".to_string()
                } else {
                    let key = parts[1];
                    let responsible_nodes = node.cluster.get_node_for_key(key).await;
                    if let Some((primary, backup)) = responsible_nodes {
                        if primary.id == node.id || backup.id == node.id {
                            let data = node.data.lock().await;
                            data.get(key).cloned().unwrap_or_else(|| "NOT_FOUND".to_string())
                        } else {
                            format!("REDIRECT:{}", primary.address)
                        }
                    } else {
                        "ERROR: No responsible node found".to_string()
                    }
                }
            },
            "SET" => {
                if parts.len() != 3 {
                    "ERROR: Invalid SET command".to_string()
                } else {
                    let key = parts[1];
                    let value = parts[2];
                    let responsible_nodes = node.cluster.get_node_for_key(key).await;
                    if let Some((primary, backup)) = responsible_nodes {
                        if primary.id == node.id {
                            {
                                let mut data = node.data.lock().await;
                                data.insert(key.to_string(), value.to_string());
                            }
                            
                            // Replicate to backup node
                            if let Err(e) = node.replicate_to_backup(key, value, &backup.address).await {
                                error!("Failed to replicate to backup: {}", e);
                                "ERROR: Replication failed".to_string()
                            } else {
                                "OK".to_string()
                            }
                        } else {
                            format!("REDIRECT:{}", primary.address)
                        }
                    } else {
                        "ERROR: No responsible node found".to_string()
                    }
                }
            },
            "DELETE" => {
                if parts.len() != 2 {
                    "ERROR: Invalid DELETE command".to_string()
                } else {
                    let key = parts[1];
                    let responsible_nodes = node.cluster.get_node_for_key(key).await;
                    if let Some((primary, backup)) = responsible_nodes {
                        if primary.id == node.id {
                            let deleted = {
                                let mut data = node.data.lock().await;
                                data.remove(key).is_some()
                            };
                            if deleted {
                                // Replicate deletion to backup node
                                if let Err(e) = node.replicate_to_backup(key, "DELETE", &backup.address).await {
                                    error!("Failed to replicate deletion to backup: {}", e);
                                    "ERROR: Replication of deletion failed".to_string()
                                } else {
                                    "OK".to_string()
                                }
                            } else {
                                "NOT_FOUND".to_string()
                            }
                        } else {
                            format!("REDIRECT:{}", primary.address)
                        }
                    } else {
                        "ERROR: No responsible node found".to_string()
                    }
                }
            },
            "REPLICATE" => {
                if parts.len() != 3 {
                    "ERROR: Invalid REPLICATE command".to_string()
                } else {
                    let key = parts[1];
                    let value = parts[2];
                    let mut data = node.data.lock().await;
                    if value == "DELETE" {
                        data.remove(key);
                    } else {
                        data.insert(key.to_string(), value.to_string());
                    }
                    "OK".to_string()
                }
            },
            _ => "ERROR: Unknown command".to_string(),
        };

        socket.write_all(response.as_bytes()).await?;
    }
}