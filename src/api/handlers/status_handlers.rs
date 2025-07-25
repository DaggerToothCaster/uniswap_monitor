use super::super::ApiState;
use crate::database::operations::{EventOperations, StatsOperations};
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ChainQuery {
    pub chain_id: Option<i32>,
}

use super::ApiResponse;

// Status相关handlers
pub async fn get_processing_status(
    Query(params): Query<ChainQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let chain_id = params.chain_id;
    match StatsOperations::get_processing_status(state.database.pool(), chain_id).await {
        Ok(status) => Ok(ApiResponse::success(status)),
        Err(e) => {
            let error_msg = format!("Failed to get processing status: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}

pub async fn get_detailed_processing_status(
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match EventOperations::get_all_last_processed_blocks(state.database.pool()).await {
        Ok(blocks) => Ok(ApiResponse::success(blocks)),
        Err(e) => {
            let error_msg = format!("Failed to get detailed processing status: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}

pub async fn get_chain_stats(
    Query(params): Query<ChainQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match StatsOperations::get_chain_stats(state.database.pool(), params.chain_id).await {
        Ok(stats) => Ok(ApiResponse::success(stats)),
        Err(e) => {
            let error_msg = format!("Failed to get chain stats: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}

pub async fn get_system_health(
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match StatsOperations::get_system_health(state.database.pool()).await {
        Ok(health) => Ok(ApiResponse::success(health)),
        Err(e) => {
            let error_msg = format!("Failed to get system health: {}", e);
            tracing::error!("{}", error_msg);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error_msg,
            ))
        }
    }
}
