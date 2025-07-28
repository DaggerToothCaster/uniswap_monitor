use super::super::ApiState;
use crate::database::operations::TradingOperations;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::json;

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

/// 获取交易对列表API接口
///
/// # 参数
/// * `params` - 查询参数，包含以下字段：
///   - `chain_id`: 可选，链ID筛选条件
///   - `limit`: 可选，每页记录数
///   - `offset`: 可选，分页偏移量
/// * `state` - 应用状态，包含数据库连接池
///
/// # 返回值
/// 返回标准API响应格式：
/// - 成功时返回HTTP 200，包含以下数据结构：
///   ```json
///   {
///     "code": 200,
///     "message": "success",
///     "data": {
///       "data": [...],  // 交易对列表
///       "pagination": {
///         "total": 100, // 总记录数
///         "limit": 10,  // 每页记录数
///         "offset": 0   // 当前偏移量
///       }
///     }
///   }
///   ```
/// - 失败时返回message，包含错误信息
///
/// # 错误信息
/// - message: 数据库查询失败
///
/// # 示例请求
/// ```
/// GET /api/pairs?chain_id=1&limit=10&offset=0
/// ```
///
/// # 注意事项
/// 1. 当chain_id为0或未提供时，返回所有链的交易对
/// 2. 当limit未提供时，默认返回所有记录
/// 3. 当offset未提供时，默认从第一条记录开始
/// 获取交易对列表API接口（带统计信息）
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
        Ok((pairs, total)) => {
            let response = json!({
                "data": pairs,
                "pagination": {
                    "total": total,
                    "limit": params.limit.unwrap_or_default(),
                    "offset": params.offset.unwrap_or_default()
                }
            });
            Ok(ApiResponse::success(response))
        }
        Err(e) => {
            let error_msg = format!("Failed to get pairs: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
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
