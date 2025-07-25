use super::super::ApiState;
use crate::database::operations::MetadataOperations;
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;


#[derive(Debug, Deserialize)]
pub struct TokenMetadataQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub verified_only: Option<bool>,
}

use super::ApiResponse;

// Token metadata相关handlers
pub async fn update_token_metadata(
    State(state): State<ApiState>,
    Json(payload): Json<UpdateTokenMetadata>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match MetadataOperations::upsert_token_metadata(
        state.database.pool(),
        &payload,
    )
    .await
    {
        Ok(Some(metadata)) => Ok(ApiResponse::success(metadata)),
        Ok(None) => Err(ApiResponse::<()>::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create token metadata".to_string(),
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

pub async fn get_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match MetadataOperations::get_token_metadata(state.database.pool(), chain_id, &address.to_lowercase()).await {
        Ok(Some(metadata)) => Ok(ApiResponse::success(metadata)),
        Ok(None) => Err(ApiResponse::<()>::error(
            StatusCode::NOT_FOUND,
            "Token metadata not found".to_string(),
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

pub async fn delete_token_metadata(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    match MetadataOperations::delete_token_metadata(state.database.pool(), chain_id, &address.to_lowercase()).await
    {
        Ok(true) => {
            // 发送WebSocket通知
            let _ = state.event_sender.send(format!(
                "{{\"type\":\"token_metadata_deleted\",\"data\":{{\"chain_id\":{},\"address\":\"{}\"}}}}",
                chain_id, address
            ));
            Ok(ApiResponse::success("Token metadata deleted successfully"))
        }
        Ok(false) => Err(ApiResponse::<()>::error(
            StatusCode::NOT_FOUND,
            "Token metadata not found".to_string(),
        )),
        Err(e) => {
            tracing::error!("Failed to delete token metadata: {}", e);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete token metadata: {}", e),
            ))
        }
    }
}

pub async fn list_token_metadata(
    Query(params): Query<TokenMetadataQuery>,
    State(state): State<ApiState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match MetadataOperations::list_token_metadata(
        state.database.pool(),
        params.chain_id,
        limit,
        offset,
    )
    .await
    {
        Ok(metadata_list) => Ok(ApiResponse::success(metadata_list)),
        Err(e) => {
            tracing::error!("Failed to list token metadata: {}", e);
            Err(ApiResponse::<()>::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list token metadata: {}", e),
            ))
        }
    }
}
