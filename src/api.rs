use crate::database::Database;
use crate::models::*;
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
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

pub struct ApiState {
    pub database: Arc<Database>,
    pub event_sender: broadcast::Sender<String>,
}

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/pairs", get(get_pairs))
        .route("/api/pairs/:chain_id/:address/kline", get(get_kline))
        .route("/api/tokens", get(get_token_list))
        .route("/api/chains/stats", get(get_chain_stats))
        .route("/api/ws", get(websocket_handler))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

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

async fn websocket_handler(
    ws: WebSocketUpgrade,
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
