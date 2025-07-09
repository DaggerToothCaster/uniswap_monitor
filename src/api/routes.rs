use super::{handlers, websocket, ApiState};
use axum::{
    routing::{get, post, put, delete},
    Router,
};
use tower_http::cors::CorsLayer;

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Trading pairs routes
        .route("/api/pairs", get(handlers::get_pairs))
        .route("/api/pairs/:chain_id/:address/kline", get(handlers::get_kline))
        .route("/api/pairs/:chain_id/:address/timeseries", get(handlers::get_timeseries))  // 新增分时图接口
        .route("/api/pairs/:chain_id/:address/trades", get(handlers::get_pair_trades))
        .route("/api/pairs/:chain_id/:address/liquidity", get(handlers::get_pair_liquidity))
        
        // Token routes
        .route("/api/tokens", get(handlers::get_token_list))
        
        // Wallet routes
        .route("/api/wallets/:address/transactions", get(handlers::get_wallet_transactions))
        
        // Chain stats routes
        .route("/api/chains/stats", get(handlers::get_chain_stats))
        
        // Token metadata management routes
        .route("/api/metadata/tokens", get(handlers::list_token_metadata).post(handlers::create_token_metadata))
        .route("/api/metadata/tokens/:chain_id/:address", 
               get(handlers::get_token_metadata)
               .put(handlers::update_token_metadata)
               .delete(handlers::delete_token_metadata))
        .route("/api/metadata/tokens/:chain_id/:address/detail", get(handlers::get_token_detail))
        
        // Status routes - 修改和新增
        .route("/api/status/blocks", get(handlers::get_processing_status))  // 返回处理状态视图
        .route("/api/status/blocks/detailed", get(handlers::get_detailed_processing_status))  // 返回详细记录
        
        // WebSocket
        .route("/api/ws", get(websocket::websocket_handler))
        
        .layer(CorsLayer::permissive())
        .with_state(state)
}
