use crate::database::Database;
use crate::event_listener::EventListener;
use crate::models::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{delete, get, post, put},
    Router,
};
use ethers::{
    providers::{Http, Provider},
    types::Address,
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

#[derive(Debug, Deserialize)]
pub struct ProcessBlockRangeRequest {
    pub chain_id: i32,
    pub from_block: u64,
    pub to_block: u64,
}

#[derive(Debug, Serialize)]
pub struct ProcessBlockRangeResponse {
    pub success: bool,
    pub message: String,
    pub processed_blocks: u64,
}

pub struct ApiState {
    pub database: Arc<Database>,
    pub event_sender: broadcast::Sender<String>,
    pub providers: std::collections::HashMap<u64, Arc<Provider<Http>>>,
    pub factory_addresses: std::collections::HashMap<u64, Address>,
}

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Existing routes
        .route("/api/pairs", get(get_pairs))
        .route("/api/pairs/:chain_id/:address/kline", get(get_kline))
        .route("/api/tokens", get(get_token_list))
        .route("/api/chains/stats", get(get_chain_stats))
        // Token metadata management routes
        .route(
            "/api/metadata/tokens",
            get(list_token_metadata).post(create_token_metadata),
        )
        .route(
            "/api/metadata/tokens/:chain_id/:address",
            get(get_token_metadata)
                .put(update_token_metadata)
                .delete(delete_token_metadata),
        )
        .route(
            "/api/metadata/tokens/:chain_id/:address/detail",
            get(get_token_detail),
        )
        // 新增：手动处理区块范围的API
        .route("/api/process/blocks", post(process_block_range))
        // WebSocket
        .route("/api/ws", get(websocket_handler))
        // 在现有的 API 路由中添加一个新的端点来查看处理状态
        .route("/api/status/blocks", get(get_processing_status))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

// 新增：处理指定区块范围的API端点
async fn process_block_range(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<ProcessBlockRangeRequest>,
) -> Result<Json<ProcessBlockRangeResponse>, StatusCode> {
    // 验证参数
    if payload.from_block > payload.to_block {
        return Ok(Json(ProcessBlockRangeResponse {
            success: false,
            message: "from_block 不能大于 to_block".to_string(),
            processed_blocks: 0,
        }));
    }

    let chain_id = payload.chain_id as u64;

    // 获取对应链的 provider 和 factory 地址
    let provider = match state.providers.get(&chain_id) {
        Some(provider) => Arc::clone(provider),
        None => {
            return Ok(Json(ProcessBlockRangeResponse {
                success: false,
                message: format!("链 {} 未配置或不支持", chain_id),
                processed_blocks: 0,
            }));
        }
    };

    let factory_address = match state.factory_addresses.get(&chain_id) {
        Some(address) => *address,
        None => {
            return Ok(Json(ProcessBlockRangeResponse {
                success: false,
                message: format!("链 {} 的工厂地址未配置", chain_id),
                processed_blocks: 0,
            }));
        }
    };

    // 创建临时的 EventListener 来处理指定区块范围
    let event_listener = EventListener::new(
        provider,
        Arc::clone(&state.database),
        chain_id,
        factory_address,
        state.event_sender.clone(),
        12,                 // 临时轮询间隔，这里不会用到
        payload.from_block, // 临时起始区块，这里不会用到
        100,
        100,
    );

    // 处理指定区块范围
    match event_listener
        .process_block_range(payload.from_block, payload.to_block)
        .await
    {
        Ok(_) => {
            let processed_blocks = payload.to_block - payload.from_block + 1;
            Ok(Json(ProcessBlockRangeResponse {
                success: true,
                message: format!(
                    "成功处理链 {} 的区块 {} 到 {}，共 {} 个区块",
                    chain_id, payload.from_block, payload.to_block, processed_blocks
                ),
                processed_blocks,
            }))
        }
        Err(e) => Ok(Json(ProcessBlockRangeResponse {
            success: false,
            message: format!("处理失败: {}", e),
            processed_blocks: 0,
        })),
    }
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

    match state
        .database
        .get_kline_data(&address, chain_id, &interval, limit)
        .await
    {
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
    match state
        .database
        .update_token_metadata(chain_id, &address, &payload)
        .await
    {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn delete_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<Arc<ApiState>>,
) -> Result<StatusCode, StatusCode> {
    match state
        .database
        .delete_token_metadata(chain_id, &address)
        .await
    {
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

    match state
        .database
        .list_token_metadata(params.chain_id, limit, offset)
        .await
    {
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

async fn handle_websocket(mut socket: axum::extract::ws::WebSocket, state: Arc<ApiState>) {
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

// 添加新的处理函数
async fn get_processing_status(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<LastProcessedBlock>>, StatusCode> {
    match state.database.get_all_last_processed_blocks().await {
        Ok(blocks) => Ok(Json(blocks)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
