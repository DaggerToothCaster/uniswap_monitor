use super::super::ApiState;
use crate::database::operations::TradingOperations;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
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

// 复用之前定义的 ApiResponse 结构体
use super::ApiResponse;

/// 交易对-列表
pub async fn get_pairs(
    Query(params): Query<PairsQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match TradingOperations::get_all_pairs(
        state.database.pool(),
        params.chain_id,
        params.limit,
        params.offset,
    )
    .await
    {
        Ok(pairs) => Ok(ApiResponse::success(pairs)),
        Err(e) => {
            let error_msg = format!("Failed to get pairs: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_pair_detail(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match TradingOperations::get_pair_detail(state.database.pool(), &address, chain_id).await {
        Ok(Some(detail)) => Ok(ApiResponse::success(detail)),
        Ok(None) => Err(ApiResponse::<()>::error(
            StatusCode::NOT_FOUND,
            "Pair not found".to_string(),
        )),
        Err(e) => {
            let error_msg = format!("Database error: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_kline(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<KlineQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let interval = params.interval.unwrap_or_else(|| "1h".to_string());
    let limit = params.limit.unwrap_or(100);

    if !is_valid_interval(&interval) {
        return Err(ApiResponse::<()>::error(
            StatusCode::BAD_REQUEST,
            "Invalid interval parameter".to_string(),
        ));
    }

    match TradingOperations::get_kline_data(
        state.database.pool(),
        &address,
        chain_id,
        &interval,
        limit,
    )
    .await
    {
        Ok(klines) => Ok(ApiResponse::success(klines)),
        Err(e) => {
            let error_msg = format!("Failed to get kline data: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_timeseries(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<TimeSeriesQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let hours = params.hours.unwrap_or(24);

    if hours > 168 {
        return Err(ApiResponse::<()>::error(
            StatusCode::BAD_REQUEST,
            "Maximum time range is 168 hours (7 days)".to_string(),
        ));
    }

    match TradingOperations::get_timeseries_data(state.database.pool(), &address, chain_id, hours)
        .await
    {
        Ok(timeseries) => Ok(ApiResponse::success(timeseries)),
        Err(e) => {
            let error_msg = format!("Failed to get timeseries data: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_pair_trades(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<TradeQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match TradingOperations::get_pair_trades(
        state.database.pool(),
        &address,
        chain_id,
        limit,
        offset,
    )
    .await
    {
        Ok(trades) => Ok(ApiResponse::success(trades)),
        Err(e) => {
            let error_msg = format!("Failed to get pair trades: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_pair_liquidity(
    Path((chain_id, address)): Path<(i32, String)>,
    Query(params): Query<LiquidityQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match TradingOperations::get_pair_liquidity_events(
        state.database.pool(),
        &address,
        chain_id,
        limit,
        offset,
    )
    .await
    {
        Ok(liquidity) => Ok(ApiResponse::success(liquidity)),
        Err(e) => {
            let error_msg = format!("Failed to get pair liquidity: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

pub async fn get_pair_stats(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match TradingOperations::get_pair_stats(state.database.pool(), &address, chain_id).await {
        Ok(Some(stats)) => Ok(ApiResponse::success(stats)),
        Ok(None) => Err(ApiResponse::<()>::error(
            StatusCode::NOT_FOUND,
            "Pair stats not found".to_string(),
        )),
        Err(e) => {
            let error_msg = format!("Failed to get pair stats: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

// 辅助函数：验证时间区间参数
fn is_valid_interval(interval: &str) -> bool {
    matches!(
        interval,
        "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d" | "1w" | "1M" | "1y"
    )
}