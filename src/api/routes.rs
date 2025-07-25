use super::{handlers, websocket, ApiState};
use axum::{
    routing::{delete, get, post, put},
    Router,
};
use tower_http::cors::CorsLayer;

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Trading pairs routes
        .route("/api/pairs", get(handlers::get_pairs))
        .route(
            "/api/pairs/:chain_id/:address",
            get(handlers::get_pair_detail),
        )
        .route(
            "/api/pairs/:chain_id/:address/kline",
            get(handlers::get_kline),
        )
        .route(
            "/api/pairs/:chain_id/:address/timeseries",
            get(handlers::get_timeseries),
        )
        .route(
            "/api/pairs/:chain_id/:address/trades",
            get(handlers::get_pair_trades),
        )
        .route(
            "/api/pairs/:chain_id/:address/liquidity",
            get(handlers::get_pair_liquidity),
        )
        .route(
            "/api/pairs/:chain_id/:address/stats",
            get(handlers::get_pair_stats),
        )


        // Token routes
        .route("/api/tokens", get(handlers::get_token_list))
        .route("/api/tokens/search", get(handlers::search_tokens))
        .route("/api/tokens/trending", get(handlers::get_trending_tokens))
        .route("/api/tokens/new", get(handlers::get_new_tokens))
        .route(
            "/api/tokens/:chain_id/:address",
            get(handlers::get_token_detail),
        )


        // Wallet routes
        .route(
            "/api/wallets/:address/transactions",
            get(handlers::get_wallet_transactions),
        )
        .route(
            "/api/wallets/:address/stats",
            get(handlers::get_wallet_stats),
        )
        

        // Token metadata management routes
        .route(
            "/api/metadata/tokens",
            get(handlers::list_token_metadata).put(handlers::update_token_metadata),
        )
        .route(
            "/api/metadata/tokens/:chain_id/:address",
            get(handlers::get_token_metadata).delete(handlers::delete_token_metadata),
        )
        
        
        // Status routes
        .route("/api/status/chains", get(handlers::get_chain_stats))
        .route("/api/status/health", get(handlers::get_system_health))
        .route("/api/status/blocks", get(handlers::get_processing_status))
        .route(
            "/api/status/blocks/detailed",
            get(handlers::get_detailed_processing_status),
        )
        
        
        // WebSocket
        .route("/api/ws", get(websocket::websocket_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
