pub mod handlers;
pub mod routes;
pub mod websocket;

use crate::database::Database;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ApiState {
    pub database: Database,
    pub event_sender: broadcast::Sender<String>,
}

impl ApiState {
    pub fn new(database: Database) -> Self {
        let (event_sender, _) = broadcast::channel(1000);
        Self {
            database,
            event_sender,
        }
    }
}
