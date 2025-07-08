use crate::{api::{create_router, ApiState}, config::Config, database::Database};
use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub struct ApiService {
    config: Config,
    database: Arc<Database>,
    event_sender: broadcast::Sender<String>,
}

impl ApiService {
    pub async fn new(config: Config, event_sender: Option<broadcast::Sender<String>>) -> Result<Self> {
        let pool = PgPool::connect(&config.database.url).await?;
        let database = Arc::new(Database::new(pool));

        let event_sender = event_sender.unwrap_or_else(|| {
            let (sender, _) = broadcast::channel(1000);
            sender
        });

        Ok(Self {
            config,
            database,
            event_sender,
        })
    }

    pub async fn start(&self) -> Result<()> {
        info!("🚀 启动API服务...");

        let api_state = ApiState::new(
            Arc::clone(&self.database),
            self.event_sender.clone(),
        );

        let app = create_router(api_state);
        let listener = tokio::net::TcpListener::bind(format!("{}:{}", self.config.server.host, self.config.server.port)).await?;
        
        info!("API Server starting on {}:{}", self.config.server.host, self.config.server.port);
        axum::serve(listener, app).await?;

        Ok(())
    }
}
