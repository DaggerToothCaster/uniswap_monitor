mod api;
mod config;
mod database;
mod event_listener;
mod models;

use crate::{
    api::{create_router, ApiState},
    config::Config,
    database::Database,
    event_listener::EventListener,
};
use anyhow::Result;
use ethers::{providers::{Provider, Http}, types::Address};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded successfully");

    // Connect to database
    let pool = PgPool::connect(&config.database.url).await?;
    let database = Arc::new(Database::new(pool));
    database.create_tables().await?;
    info!("Database connected and tables created");

    // Create event broadcast channel
    let (event_sender, _) = broadcast::channel(1000);

    // Start event listeners for each enabled chain
    for (chain_id, chain_config) in &config.chains {
        if !chain_config.enabled {
            info!("Chain {} ({}) is disabled, skipping", chain_id, chain_config.name);
            continue;
        }

        info!("Starting monitoring for chain {} ({})", chain_id, chain_config.name);

        let provider = Arc::new(Provider::<Http>::try_from(&chain_config.rpc_url)?);
        let factory_address: Address = chain_config.factory_address.parse()?;
        
        let mut event_listener = EventListener::new(
            provider,
            Arc::clone(&database),
            *chain_id,
            factory_address,
            event_sender.clone(),
            chain_config.poll_interval,
            chain_config.start_block,
        );

        tokio::spawn(async move {
            if let Err(e) = event_listener.start_monitoring().await {
                tracing::error!("Event listener error for chain {}: {}", chain_id, e);
            }
        });
    }

    // Start API server
    let api_state = ApiState {
        database: Arc::clone(&database),
        event_sender,
    };

    let app = create_router(api_state);
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", config.server.host, config.server.port)).await?;
    
    info!("Server starting on {}:{}", config.server.host, config.server.port);
    axum::serve(listener, app).await?;

    Ok(())
}
