pub mod handlers;
pub mod websocket;
pub mod routes;

pub use routes::create_router;

use crate::database::Database;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ApiState {
    pub database: Arc<Database>,
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
