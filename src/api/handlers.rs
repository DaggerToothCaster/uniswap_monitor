use super::ApiState;
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct KlineQuery {
    pub interval: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct TimeSeriesQuery {
    pub hours: Option<i32>,
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
pub struct TradeQuery {
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct LiquidityQuery {
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct WalletQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}

// Trading pairs handlers
pub async fn get_pairs(
    Query(params): Query<PairsQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TradingPair>>, StatusCode> {
    match crate::database::operations::get_all_pairs(state.database.pool(), params.chain_id).await {
        Ok(pairs) => Ok(Json(pairs)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_kline(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<KlineQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<KlineData>>, StatusCode> {
    let interval = params.interval.unwrap_or_else(|| "1h".to_string());
    let limit = params.limit.unwrap_or(100);

    // 验证时间区间参数
    if !is_valid_interval(&interval) {
        return Err(StatusCode::BAD_REQUEST);
    }

    match crate::database::operations::get_kline_data(state.database.pool(), &address, chain_id, &interval, limit).await {
        Ok(klines) => Ok(Json(klines)),
        Err(e) => {
            tracing::error!("Failed to get kline data: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_timeseries(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<TimeSeriesQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TimeSeriesData>>, StatusCode> {
    let hours = params.hours.unwrap_or(24);

    // 限制查询范围
    if hours > 168 { // 最多7天
        return Err(StatusCode::BAD_REQUEST);
    }

    match crate::database::operations::get_timeseries_data(state.database.pool(), &address, chain_id, hours).await {
        Ok(timeseries) => Ok(Json(timeseries)),
        Err(e) => {
            tracing::error!("Failed to get timeseries data: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_pair_trades(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<TradeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TradeRecord>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match crate::database::operations::get_pair_trades(state.database.pool(), &address, chain_id, limit, offset).await {
        Ok(trades) => Ok(Json(trades)),
        Err(e) => {
            tracing::error!("Failed to get pair trades: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_pair_liquidity(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<LiquidityQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<LiquidityRecord>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    // This would need to be implemented in database operations
    // For now, return empty vec
    Ok(Json(vec![]))
}

// Token handlers
pub async fn get_token_list(
    Query(params): Query<TokenListQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(100);

    // This would need to be implemented in database operations
    // For now, return empty vec
    Ok(Json(vec![]))
}

// Wallet handlers
pub async fn get_wallet_transactions(
    Path(address): Path<String>,
    Query(params): Query<WalletQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<WalletTransaction>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    // This would need to be implemented in database operations
    // For now, return empty vec
    Ok(Json(vec![]))
}

// Chain stats handlers
pub async fn get_chain_stats(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ChainStats>>, StatusCode> {
    // This would need to be implemented in database operations
    // For now, return empty vec
    Ok(Json(vec![]))
}

// Token metadata handlers
pub async fn create_token_metadata(
    State(state): State<ApiState>,
    Json(payload): Json<CreateTokenMetadata>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match crate::database::operations::create_token_metadata(state.database.pool(), &payload).await {
        Ok(metadata) => Ok(Json(metadata)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match crate::database::operations::get_token_metadata(state.database.pool(), chain_id, &address).await {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn update_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
    Json(payload): Json<UpdateTokenMetadata>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    // This would need to be implemented in database operations
    Err(StatusCode::NOT_IMPLEMENTED)
}

pub async fn delete_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<StatusCode, StatusCode> {
    // This would need to be implemented in database operations
    Err(StatusCode::NOT_IMPLEMENTED)
}

pub async fn list_token_metadata(
    Query(params): Query<TokenMetadataQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenMetadata>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    // This would need to be implemented in database operations
    Ok(Json(vec![]))
}

pub async fn get_token_detail(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<TokenDetail>, StatusCode> {
    // This would need to be implemented in database operations
    Err(StatusCode::NOT_IMPLEMENTED)
}

// Status handlers - 修改为返回新的处理状态
pub async fn get_processing_status(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ProcessingStatus>>, StatusCode> {
    match crate::database::operations::get_processing_status(state.database.pool()).await {
        Ok(status) => Ok(Json(status)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// 新增：获取详细的区块处理记录
pub async fn get_detailed_processing_status(
    State(state): State<ApiState>,
) -> Result<Json<Vec<LastProcessedBlock>>, StatusCode> {
    match crate::database::operations::get_all_last_processed_blocks(state.database.pool()).await {
        Ok(blocks) => Ok(Json(blocks)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// 辅助函数：验证时间区间参数
fn is_valid_interval(interval: &str) -> bool {
    matches!(interval, "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d" | "1w" | "1M" | "1y")
}
