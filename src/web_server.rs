// src/web_server.rs

use axum::{
    extract::{Path, Query, State},
    http::{StatusCode, HeaderMap},
    response::{Html, Json},
    routing::{get, post, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::client::{Client, KeyValue};
use crate::cluster::Cluster;

#[derive(Clone)]
pub struct AppState {
    client: Arc<Client>,
    cluster: Arc<Cluster>,
}

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct SetKeyRequest {
    value: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    nodes: usize,
    uptime: String,
}

pub struct WebServer {
    state: AppState,
}

impl WebServer {
    pub fn new(client: Arc<Client>, cluster: Arc<Cluster>) -> Self {
        Self {
            state: AppState { client, cluster },
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let app = Router::new()
            .route("/", get(serve_dashboard))
            .route("/api/health", get(health_check))
            .route("/api/keys", get(list_keys))
            .route("/api/keys/:key", get(get_key))
            .route("/api/keys/:key", post(set_key))
            .route("/api/keys/:key", delete(delete_key))
            .route("/api/cluster/stats", get(cluster_stats))
            .route("/api/cluster/nodes", get(cluster_nodes))
            .route("/api/cluster/distribution", get(key_distribution))
            .nest_service("/static", ServeDir::new("static"))
            .layer(CorsLayer::permissive())
            .with_state(self.state.clone());

        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
        println!("🌐 Web server listening on http://0.0.0.0:3000");

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn serve_dashboard() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn health_check(State(state): State<AppState>) -> Json<ApiResponse<HealthResponse>> {
    let stats = state.cluster.get_stats().await;
    Json(ApiResponse {
        success: true,
        data: Some(HealthResponse {
            status: "healthy".to_string(),
            nodes: stats.total_nodes,
            uptime: "Running".to_string(),
        }),
        error: None,
    })
}

async fn list_keys(State(state): State<AppState>) -> Json<ApiResponse<Vec<String>>> {
    match state.client.list_keys().await {
        Ok(keys) => Json(ApiResponse {
            success: true,
            data: Some(keys),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}

async fn get_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> Json<ApiResponse<Option<String>>> {
    match state.client.get(&key).await {
        Ok(value) => Json(ApiResponse {
            success: true,
            data: Some(value),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}

async fn set_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SetKeyRequest>,
) -> Json<ApiResponse<()>> {
    match state.client.set(&key, &payload.value).await {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: Some(()),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}

async fn delete_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> Json<ApiResponse<bool>> {
    match state.client.delete(&key).await {
        Ok(deleted) => Json(ApiResponse {
            success: true,
            data: Some(deleted),
            error: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        }),
    }
}

async fn cluster_stats(State(state): State<AppState>) -> Json<ApiResponse<crate::cluster::ClusterStats>> {
    let stats = state.cluster.get_stats().await;
    Json(ApiResponse {
        success: true,
        data: Some(stats),
        error: None,
    })
}

async fn cluster_nodes(State(state): State<AppState>) -> Json<ApiResponse<Vec<crate::cluster::ClusterNode>>> {
    let nodes = state.cluster.get_nodes().await;
    Json(ApiResponse {
        success: true,
        data: Some(nodes),
        error: None,
    })
}

async fn key_distribution(State(state): State<AppState>) -> Json<ApiResponse<Vec<(String, String)>>> {
    let distribution = state.cluster.get_key_distribution().await;
    Json(ApiResponse {
        success: true,
        data: Some(distribution),
        error: None,
    })
}