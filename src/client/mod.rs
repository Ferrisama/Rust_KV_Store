// src/client/mod.rs

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;
use std::future::Future;
use std::pin::Pin;
use serde::{Deserialize, Serialize};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse server response")]
    ParseError,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Too many redirects")]
    TooManyRedirects,
}

#[derive(Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub struct Client {
    addresses: Vec<String>,
}

impl Client {
    pub fn new(addresses: Vec<String>) -> Self {
        Client { addresses }
    }

    fn send_command<'a>(&'a self, command: String, redirects: u8) -> Pin<Box<dyn Future<Output = Result<String, ClientError>> + Send + 'a>> {
        Box::pin(async move {
            if redirects > 5 {
                return Err(ClientError::TooManyRedirects);
            }

            for address in &self.addresses {
                let mut stream = match TcpStream::connect(address).await {
                    Ok(stream) => stream,
                    Err(_) => continue,
                };

                if let Err(_) = stream.write_all(command.as_bytes()).await {
                    continue;
                }

                let mut buffer = [0; 1024];
                let n = match stream.read(&mut buffer).await {
                    Ok(n) => n,
                    Err(_) => continue,
                };

                let response = String::from_utf8_lossy(&buffer[..n]).to_string();

                if response.starts_with("REDIRECT:") {
                    let new_address = response.strip_prefix("REDIRECT:").unwrap().trim().to_string();
                    let mut new_addresses = self.addresses.clone();
                    if !new_addresses.contains(&new_address) {
                        new_addresses.push(new_address);
                    }
                    let new_client = Client::new(new_addresses);
                    return new_client.send_command(command, redirects + 1).await;
                }

                if response.starts_with("ERROR:") {
                    return Err(ClientError::ServerError(response));
                }

                return Ok(response.trim().to_string());
            }

            Err(ClientError::ServerError("No available nodes".to_string()))
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, ClientError> {
        let response = self.send_command(format!("GET {}", key), 0).await?;
        match response.as_str() {
            "NOT_FOUND" => Ok(None),
            value => Ok(Some(value.to_string())),
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), ClientError> {
        let response = self.send_command(format!("SET {} {}", key, value), 0).await?;
        if response == "OK" {
            Ok(())
        } else {
            Err(ClientError::ParseError)
        }
    }

    pub async fn delete(&self, key: &str) -> Result<bool, ClientError> {
        let response = self.send_command(format!("DELETE {}", key), 0).await?;
        match response.as_str() {
            "OK" => Ok(true),
            "NOT_FOUND" => Ok(false),
            _ => Err(ClientError::ParseError),
        }
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, ClientError> {
        // This is a simplified implementation - in a real system you'd want to
        // query all nodes and aggregate results
        let response = self.send_command("LIST".to_string(), 0).await?;
        if response == "NOT_IMPLEMENTED" {
            return Ok(Vec::new());
        }
        Ok(response.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
    }
}