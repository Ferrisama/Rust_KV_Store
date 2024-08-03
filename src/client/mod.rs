// src/client/mod.rs

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;
use std::future::Future;
use std::pin::Pin;

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

                stream.write_all(command.as_bytes()).await?;

                let mut buffer = [0; 1024];
                let n = stream.read(&mut buffer).await?;
                let response = String::from_utf8_lossy(&buffer[..n]).to_string();

                if response.starts_with("REDIRECT:") {
                    let new_address = response.strip_prefix("REDIRECT:").unwrap().to_string();
                    let mut new_addresses = self.addresses.clone();
                    new_addresses.push(new_address);
                    let new_client = Client::new(new_addresses);
                    return new_client.send_command(command, redirects + 1).await;
                }

                if response.starts_with("ERROR:") {
                    return Err(ClientError::ServerError(response));
                }

                return Ok(response);
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
}