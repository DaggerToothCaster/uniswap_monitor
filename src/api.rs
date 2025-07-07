use crate::database::Database;
use crate::models::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post, put, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

#[derive(Debug, Deserialize)]
pub struct KlineQuery {
    pub interval: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct TokenListQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct PairsQuery {
    pub chain_id: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct TokenMetadataQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

pub struct ApiState {
    pub database: Arc<Database>,
    pub event_sender: broadcast::Sender<String>,
}

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Existing routes
        .route("/api/pairs", get(get_pairs))
        .route("/api/pairs/:chain_id/:address/kline", get(get_kline))
        .route("/api/tokens", get(get_token_list))
        .route("/api/chains/stats", get(get_chain_stats))
        
        // Token metadata management routes
        .route("/api/metadata/tokens", get(list_token_metadata).post(create_token_metadata))
        .route("/api/metadata/tokens/:chain_id/:address", 
               get(get_token_metadata)
               .put(update_token_metadata)
               .delete(delete_token_metadata))
        .route("/api/metadata/tokens/:chain_id/:address/detail", get(get_token_detail))
        
        // WebSocket
        .route("/api/ws", get(websocket_handler))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

// Existing handlers...
async fn get_pairs(
    Query(params): Query<PairsQuery>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<TradingPair>>, StatusCode> {
    match state.database.get_all_pairs(params.chain_id).await {
        Ok(pairs) => Ok(Json(pairs)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_kline(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<KlineQuery>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<KlineData>>, StatusCode> {
    let interval = params.interval.unwrap_or_else(|| "1h".to_string());
    let limit = params.limit.unwrap_or(100);

    match state.database.get_kline_data(&address, chain_id, &interval, limit).await {
        Ok(klines) => Ok(Json(klines)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_token_list(
    Query(params): Query<TokenListQuery>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(100);

    match state.database.get_token_list(params.chain_id, limit).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_chain_stats(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<ChainStats>>, StatusCode> {
    match state.database.get_chain_stats().await {
        Ok(stats) => Ok(Json(stats)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// Token metadata handlers
async fn create_token_metadata(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<CreateTokenMetadata>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match state.database.create_token_metadata(&payload).await {
        Ok(metadata) => Ok(Json(metadata)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match state.database.get_token_metadata(chain_id, &address).await {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn update_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<UpdateTokenMetadata>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match state.database.update_token_metadata(chain_id, &address, &payload).await {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn delete_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<Arc<ApiState>>,
) -> Result<StatusCode, StatusCode> {
    match state.database.delete_token_metadata(chain_id, &address).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn list_token_metadata(
    Query(params): Query<TokenMetadataQuery>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<TokenMetadata>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match state.database.list_token_metadata(params.chain_id, limit, offset).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_token_detail(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<TokenDetail>, StatusCode> {
    match state.database.get_token_detail(chain_id, &address).await {
        Ok(Some(detail)) => Ok(Json(detail)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn websocket_handler(
    ws: axum::extract::WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(
    mut socket: axum::extract::ws::WebSocket,
    state: Arc<ApiState>,
) {
    let mut receiver = state.event_sender.subscribe();

    while let Ok(message) = receiver.recv().await {
        if socket
            .send(axum::extract::ws::Message::Text(message))
            .await
            .is_err()
        {
            break;
        }
    }
}
