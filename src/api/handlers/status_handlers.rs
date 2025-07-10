use super::super::ApiState;
use crate::types::*;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ChainQuery {
    pub chain_id: Option<i32>,
}

// Status相关handlers
pub async fn get_processing_status(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ProcessingStatus>>, StatusCode> {
    match crate::database::operations::get_processing_status(state.database.pool()).await {
        Ok(status) => Ok(Json(status)),
        Err(e) => {
            tracing::error!("Failed to get processing status: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_detailed_processing_status(
    State(state): State<ApiState>,
) -> Result<Json<Vec<LastProcessedBlock>>, StatusCode> {
    match crate::database::operations::get_all_last_processed_blocks(state.database.pool()).await {
        Ok(blocks) => Ok(Json(blocks)),
        Err(e) => {
            tracing::error!("Failed to get detailed processing status: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_chain_stats(
    Query(params): Query<ChainQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<ChainStats>>, StatusCode> {
    match crate::database::operations::get_chain_stats(state.database.pool(), params.chain_id).await {
        Ok(stats) => Ok(Json(stats)),
        Err(e) => {
            tracing::error!("Failed to get chain stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_system_health(
    State(state): State<ApiState>,
) -> Result<Json<SystemHealth>, StatusCode> {
    match crate::database::operations::get_system_health(state.database.pool()).await {
        Ok(health) => Ok(Json(health)),
        Err(e) => {
            tracing::error!("Failed to get system health: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
