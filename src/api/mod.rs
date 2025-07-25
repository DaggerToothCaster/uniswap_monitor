// ApiState 部分
pub mod handlers;
pub mod routes;
pub mod websocket;

use crate::database::Database;
use std::sync::Arc;
use tokio::sync::broadcast;
pub use routes::*;

#[derive(Clone)]
pub struct ApiState {
    pub database: Arc<Database>,  // 改为Arc<Database>
    pub event_sender: broadcast::Sender<String>,
}

impl ApiState {
    pub fn new(database: Arc<Database>, event_sender: broadcast::Sender<String>) -> Self {
        Self {
            database,
            event_sender,
        }
    }
}