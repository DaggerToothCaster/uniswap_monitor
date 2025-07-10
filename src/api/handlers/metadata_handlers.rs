use super::super::ApiState;
use crate::database::operations::MetadataOperations;
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TokenMetadataQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub verified_only: Option<bool>,
}

// Token metadata相关handlers
pub async fn update_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
    Json(payload): Json<UpdateTokenMetadata>,
) -> Result<StatusCode, StatusCode> {
    match MetadataOperations::update_token_metadata(
        state.database.pool(),
        chain_id,
        &address,
        &payload,
    )
    .await
    {
        Ok(metadata) => Ok(StatusCode::OK),
        Err(e) => {
            tracing::error!("Failed to create token metadata: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match MetadataOperations::get_token_metadata(state.database.pool(), chain_id, &address).await {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get token metadata: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn delete_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<StatusCode, StatusCode> {
    match MetadataOperations::delete_token_metadata(state.database.pool(), chain_id, &address).await
    {
        Ok(true) => {
            // 发送WebSocket通知
            let _ = state.event_sender.send(format!(
                "{{\"type\":\"token_metadata_deleted\",\"data\":{{\"chain_id\":{},\"address\":\"{}\"}}}}",
                chain_id, address
            ));
            Ok(StatusCode::NO_CONTENT)
        }
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to delete token metadata: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn list_token_metadata(
    Query(params): Query<TokenMetadataQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenMetadata>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    let verified_only = params.verified_only.unwrap_or(false);

    match MetadataOperations::list_token_metadata(
        state.database.pool(),
        params.chain_id,
        limit,
        offset,
        verified_only,
    )
    .await
    {
        Ok(metadata_list) => Ok(Json(metadata_list)),
        Err(e) => {
            tracing::error!("Failed to list token metadata: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn verify_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<TokenMetadata>, StatusCode> {
    match MetadataOperations::verify_token_metadata(state.database.pool(), chain_id, &address).await
    {
        Ok(Some(metadata)) => {
            // 发送WebSocket通知
            let _ = state.event_sender.send(format!(
                "{{\"type\":\"token_verified\",\"data\":{{\"chain_id\":{},\"address\":\"{}\"}}}}",
                chain_id, address
            ));
            Ok(Json(metadata))
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to verify token metadata: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
