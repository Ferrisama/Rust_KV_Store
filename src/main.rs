// src/main.rs

mod node;
mod client;
mod cluster;

use node::Node;
use client::Client;
use cluster::{Cluster, ClusterNode};
use log::{info, error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("Distributed Key-Value Store starting up...");
    info!("Logging initialized");

    let cluster = Arc::new(Cluster::new(10)); // 10 virtual nodes per physical node

    // Create and start three nodes
    let node_configs = vec![
        ("node1", "127.0.0.1:8081"),
        ("node2", "127.0.0.1:8082"),
        ("node3", "127.0.0.1:8083"),
    ];

    for (id, addr) in node_configs {
        let node = Node::new(id.to_string(), addr.to_string(), Arc::clone(&cluster));
        cluster.add_node(ClusterNode { id: id.to_string(), address: addr.to_string() }).await;
        
        let node_clone = node.clone();
        tokio::spawn(async move {
            if let Err(e) = node_clone.start().await {
                error!("Node error: {}", e);
            }
        });
    }

    // Give the nodes a moment to start up
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Create a client that knows about all nodes
    let client = Client::new(vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
        "127.0.0.1:8083".to_string(),
    ]);

    // Test replication
    println!("Testing replication...");

    // Set a value
    match client.set("key1", "value1").await {
        Ok(_) => println!("Set key1 to value1"),
        Err(e) => println!("Error setting key1: {}", e),
    }

    // Get the value from all nodes to verify replication
    for addr in &["127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083"] {
        let node_client = Client::new(vec![addr.to_string()]);
        match node_client.get("key1").await {
            Ok(value) => println!("Got value for key1 from {}: {:?}", addr, value),
            Err(e) => println!("Error getting key1 from {}: {}", addr, e),
        }
    }

    // Delete the value
    match client.delete("key1").await {
        Ok(_) => println!("Deleted key1"),
        Err(e) => println!("Error deleting key1: {}", e),
    }

    // Try to get the deleted value from all nodes
    for addr in &["127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083"] {
        let node_client = Client::new(vec![addr.to_string()]);
        match node_client.get("key1").await {
            Ok(value) => println!("Got value for key1 from {} after deletion: {:?}", addr, value),
            Err(e) => println!("Error getting key1 from {} after deletion: {}", addr, e),
        }
    }

    println!("Replication test completed. Press Ctrl+C to exit.");

    // Keep the main thread alive
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}