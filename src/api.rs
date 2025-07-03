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

pub struct ApiState {
    pub database: Arc<Database>,
    pub event_sender: broadcast::Sender<String>,
}

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/pairs", get(get_pairs))
        .route("/api/pairs/:address/kline", get(get_kline))
        .route("/api/ws", get(websocket_handler))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

async fn get_pairs(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<TradingPair>>, StatusCode> {
    match state.database.get_all_pairs().await {
        Ok(pairs) => Ok(Json(pairs)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_kline(
    Path(address): Path<String>,
    Query(params): Query<KlineQuery>,
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<KlineData>>, StatusCode> {
    let interval = params.interval.unwrap_or_else(|| "1h".to_string());
    let limit = params.limit.unwrap_or(100);

    match state.database.get_kline_data(&address, &interval, limit).await {
        Ok(klines) => Ok(Json(klines)),
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
