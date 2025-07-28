use super::super::ApiState;
use crate::database::operations::WalletOperations;
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct WalletQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub transaction_type: Option<String>, // swap, mint, burn
}

#[derive(Debug, Deserialize)]
pub struct WalletStatsQuery {
    pub chain_id: Option<i32>,
    pub days: Option<i32>,
}

// 复用之前定义的 ApiResponse 结构体
use super::ApiResponse;

/// 获取钱包交易记录API接口
///
/// # 参数
/// * `address` - 钱包地址
/// * `params` - 查询参数，包含以下字段：
///   - `chain_id`: 可选，链ID筛选条件
///   - `limit`: 可选，每页记录数，默认50，最大100
///   - `offset`: 可选，分页偏移量，默认0
///   - `transaction_type`: 可选，交易类型筛选（swap, mint, burn）
///
/// # 返回值
/// 返回标准API响应格式，包含交易记录和分页信息
///
/// # 示例请求
/// ```
/// GET /api/wallet/0x123.../transactions?chain_id=1&limit=20&offset=0&transaction_type=swap
/// ```
pub async fn get_wallet_transactions(
    Path(address): Path<String>,
    Query(params): Query<WalletQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    // 验证钱包地址格式
    if address.len() != 42 || !address.starts_with("0x") {
        return Err(ApiResponse::<()>::error(
            StatusCode::BAD_REQUEST,
            "Invalid wallet address format".to_string(),
        ));
    }

    // 验证分页参数
    if let Some(limit_val) = params.limit {
        if limit_val <= 0 || limit_val > 100 {
            return Err(ApiResponse::<()>::error(
                StatusCode::BAD_REQUEST,
                "Limit must be between 1 and 100".to_string(),
            ));
        }
    }

    if let Some(offset_val) = params.offset {
        if offset_val < 0 {
            return Err(ApiResponse::<()>::error(
                StatusCode::BAD_REQUEST,
                "Offset must be non-negative".to_string(),
            ));
        }
    }

    // 验证交易类型
    if let Some(ref tx_type) = params.transaction_type {
        if !matches!(tx_type.as_str(), "swap" | "mint" | "burn") {
            return Err(ApiResponse::<()>::error(
                StatusCode::BAD_REQUEST,
                "Invalid transaction type. Valid types: swap, mint, burn".to_string(),
            ));
        }
    }

    match WalletOperations::get_wallet_transactions(
        state.database.pool(),
        &address.to_lowercase(),
        params.chain_id,
        params.limit,
        params.offset,
        params.transaction_type.as_deref(),
    ).await {
        Ok((transactions, total)) => {
            let response = json!({
                "data": transactions,
                "pagination": {
                    "total": total,
                    "limit": params.limit.unwrap_or(50),
                    "offset": params.offset.unwrap_or(0),
                    "has_more": params.offset.unwrap_or(0) + params.limit.unwrap_or(50) < total as i32,
                    "chain_id": params.chain_id,
                    "transaction_type": params.transaction_type
                }
            });
            Ok(ApiResponse::success(response))
        }
        Err(e) => {
            let error_msg = format!("Failed to get wallet transactions: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}

/// 获取钱包统计信息API接口
///
/// # 参数
/// * `address` - 钱包地址
/// * `params` - 查询参数，包含以下字段：
///   - `chain_id`: 可选，链ID筛选条件
///   - `days`: 可选，统计天数，默认30天，最大365天
///
/// # 返回值
/// 返回标准API响应格式，包含钱包统计信息
///
/// # 示例请求
/// ```
/// GET /api/wallet/0x123.../stats?chain_id=1&days=30
/// ```
pub async fn get_wallet_stats(
    Path(address): Path<String>,
    Query(params): Query<WalletStatsQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    // 验证钱包地址格式
    if address.len() != 42 || !address.starts_with("0x") {
        return Err(ApiResponse::<()>::error(
            StatusCode::BAD_REQUEST,
            "Invalid wallet address format".to_string(),
        ));
    }

    let days = params.days.unwrap_or(30);

    // 验证天数参数
    if days <= 0 || days > 365 {
        return Err(ApiResponse::<()>::error(
            StatusCode::BAD_REQUEST,
            "Days must be between 1 and 365".to_string(),
        ));
    }

    match WalletOperations::get_wallet_stats(
        state.database.pool(),
        &address.to_lowercase(),
        params.chain_id,
        days,
    ).await {
        Ok(Some(stats)) => {
            let response = json!({
                "data": stats,
                "query_params": {
                    "chain_id": params.chain_id,
                    "days": days
                }
            });
            Ok(ApiResponse::success(response))
        }
        Ok(None) => Err(ApiResponse::<()>::error(
            StatusCode::NOT_FOUND,
            "Wallet stats not found".to_string(),
        )),
        Err(e) => {
            let error_msg = format!("Failed to get wallet stats: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}
