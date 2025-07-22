pub mod trading_handlers;
pub mod token_handlers;
pub mod wallet_handlers;
pub mod status_handlers;
pub mod metadata_handlers;

pub use trading_handlers::*;
pub use token_handlers::*;
pub use wallet_handlers::*;
pub use status_handlers::*;
pub use metadata_handlers::*;


use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use tracing::debug;
#[derive(Debug, Serialize)]
struct ApiResponse<T: Serialize> {
    success: bool,
    data: Option<T>,
    message: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    fn success(data: T) -> (StatusCode, String) {
        let response = Self {
            success: true,
            data: Some(data),
            message: None,
        };
        (StatusCode::OK, serde_json::to_string(&response).unwrap())
    }

    fn error(status: StatusCode, message: String) -> (StatusCode, String) {
        let response = Self {
            success: false,
            data: None,
            message: Some(message),
        };
        (status, serde_json::to_string(&response).unwrap())
    }
}
