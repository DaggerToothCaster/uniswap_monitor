use crate::{config::Config, database::Database, event_listener::EventListenerManager};
use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub struct EventService {
    config: Config,
    database: Arc<Database>,
    event_sender: broadcast::Sender<String>,
}

impl EventService {
    pub async fn new(config: Config) -> Result<Self> {
        let pool = PgPool::connect(&config.database.url).await?;
        let database = Arc::new(Database::new(pool));
        database.create_tables().await?;

        let (event_sender, _) = broadcast::channel(1000);

        Ok(Self {
            config,
            database,
            event_sender,
        })
    }

    pub async fn start(&self) -> Result<()> {
        info!("🚀 启动事件监听服务...");

        let listener_manager = EventListenerManager::new(
            Arc::clone(&self.database),
            self.event_sender.clone(),
        );

        let mut handles = Vec::new();

        for (chain_id, chain_config) in &self.config.chains {
            if !chain_config.enabled {
                info!("Chain {} ({}) is disabled, skipping", chain_id, chain_config.name);
                continue;
            }

            info!("Starting monitoring for chain {} ({})", chain_id, chain_config.name);

            let manager = listener_manager.clone();
            let chain_id = *chain_id;
            let config = chain_config.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = manager.start_chain_listeners(chain_id, &config).await {
                    tracing::error!("Event listener error for chain {}: {}", chain_id, e);
                }
            });

            handles.push(handle);
        }

        // Wait for all listeners
        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub fn get_event_sender(&self) -> broadcast::Sender<String> {
        self.event_sender.clone()
    }

    pub fn get_database(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }
}
