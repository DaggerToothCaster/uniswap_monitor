use super::super::ApiState;
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;
use crate::database::operations::{TokenOperations};

#[derive(Debug, Deserialize)]
pub struct TokenListQuery {
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub sort_by: Option<String>, // price, volume, market_cap
    pub order: Option<String>,   // asc, desc
}

#[derive(Debug, Deserialize)]
pub struct TokenSearchQuery {
    pub q: String,
    pub chain_id: Option<i32>,
    pub limit: Option<i32>,
}

// Token相关handlers
pub async fn get_token_list(
    Query(params): Query<TokenListQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);
    let sort_by = params.sort_by.unwrap_or_else(|| "volume_24h".to_string());
    let order = params.order.unwrap_or_else(|| "desc".to_string());

    match TokenOperations::get_token_list(
        state.database.pool(), 
        params.chain_id, 
        limit, 
        offset, 
        &sort_by, 
        &order
    ).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(e) => {
            tracing::error!("Failed to get token list: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_token_detail(
    Path((chain_id, address)): Path<(i32, String)>,
    State(state): State<ApiState>,
) -> Result<Json<TokenDetail>, StatusCode> {
    match TokenOperations::get_token_detail(state.database.pool(), chain_id, &address).await {
        Ok(Some(detail)) => Ok(Json(detail)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to get token detail: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn search_tokens(
    Query(params): Query<TokenSearchQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(20);

    if params.q.len() < 2 {
        return Err(StatusCode::BAD_REQUEST);
    }

    match TokenOperations::search_tokens(
        state.database.pool(), 
        &params.q, 
        params.chain_id, 
        limit
    ).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(e) => {
            tracing::error!("Failed to search tokens: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_trending_tokens(
    Query(params): Query<TokenListQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);

    match TokenOperations::get_trending_tokens(
        state.database.pool(), 
        params.chain_id, 
        limit
    ).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(e) => {
            tracing::error!("Failed to get trending tokens: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_new_tokens(
    Query(params): Query<TokenListQuery>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<TokenListItem>>, StatusCode> {
    let limit = params.limit.unwrap_or(50);

    match TokenOperations::get_new_tokens(
        state.database.pool(), 
        params.chain_id, 
        limit
    ).await {
        Ok(tokens) => Ok(Json(tokens)),
        Err(e) => {
            tracing::error!("Failed to get new tokens: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
