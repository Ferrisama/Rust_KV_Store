// src/main.rs

mod node;
mod client;
mod cluster;
mod web_server;

use node::Node;
use client::Client;
use cluster::{Cluster, ClusterNode};
use web_server::WebServer;
use log::{info, error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("🚀 Distributed Key-Value Store starting up...");
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
        cluster.add_node(ClusterNode { 
            id: id.to_string(), 
            address: addr.to_string() 
        }).await;
        
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
    let client = Arc::new(Client::new(vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
        "127.0.0.1:8083".to_string(),
    ]));

    // Start the web server
    let web_server = WebServer::new(client.clone(), cluster.clone());
    let web_handle = tokio::spawn(async move {
        if let Err(e) = web_server.start().await {
            error!("Web server error: {}", e);
        }
    });

    println!("✅ All services started!");
    println!("📊 Web UI available at: http://localhost:3000");
    println!("🔧 KV Store nodes running on ports 8081, 8082, 8083");
    println!("📝 Press Ctrl+C to shutdown");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    println!("🛑 Shutting down...");

    Ok(())
}