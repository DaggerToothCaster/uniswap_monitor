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

// Wallet相关handlers
pub async fn get_wallet_transactions(
    Path(address): Path<String>,
    Query(params): Query<WalletQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<WalletTransaction>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match WalletOperations::get_wallet_transactions(
        state.database.pool(),
        &address.to_lowercase(),
        params.chain_id,
        limit,
        offset,
        params.transaction_type.as_deref(),
    )
    .await
    {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => {
            tracing::error!("Failed to get wallet transactions: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_wallet_stats(
    Path(address): Path<String>,
    Query(params): Query<WalletStatsQuery>,
    State(state): State<ApiState>,
) -> Result<Json<WalletStats>, StatusCode> {
    let days = params.days.unwrap_or(30);

    match WalletOperations::get_wallet_stats(state.database.pool(), &address.to_lowercase(), params.chain_id, days)
        .await
    {
        Ok(Some(stats)) => Ok(Json(stats)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get wallet stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
