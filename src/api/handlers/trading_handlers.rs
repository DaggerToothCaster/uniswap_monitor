use super::super::ApiState;
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
pub struct PairsQuery {
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

// 交易对相关handlers
pub async fn get_pairs(
    Query(params): Query<PairsQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TradingPair>>, StatusCode> {
    match crate::database::operations::get_all_pairs(state.database.pool(), params.chain_id).await {
        Ok(pairs) => Ok(Json(pairs)),
        Err(e) => {
            tracing::error!("Failed to get pairs: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_pair_detail(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<PairDetail>, StatusCode> {
    match crate::database::operations::get_pair_detail(state.database.pool(), &address, chain_id).await {
        Ok(Some(detail)) => Ok(Json(detail)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get pair detail: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_kline(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<KlineQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<KLineData>>, StatusCode> {
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

    match crate::database::operations::get_pair_liquidity_events(state.database.pool(), &address, chain_id, limit, offset).await {
        Ok(liquidity) => Ok(Json(liquidity)),
        Err(e) => {
            tracing::error!("Failed to get pair liquidity: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_pair_stats(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<PairStats>, StatusCode> {
    match crate::database::operations::get_pair_stats(state.database.pool(), &address, chain_id).await {
        Ok(Some(stats)) => Ok(Json(stats)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get pair stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// 辅助函数：验证时间区间参数
fn is_valid_interval(interval: &str) -> bool {
    matches!(interval, "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d" | "1w" | "1M" | "1y")
}
