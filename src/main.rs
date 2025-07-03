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

    // Connect to Ethereum
    let provider = Arc::new(Provider::<Http>::try_from(&config.ethereum.rpc_url)?);
    info!("Connected to Ethereum HTTP RPC");

    // Create event broadcast channel
    let (event_sender, _) = broadcast::channel(1000);

    // Start event listener
    let factory_address: Address = config.ethereum.factory_address.parse()?;
    let mut event_listener = EventListener::new(
        Arc::clone(&provider),
        Arc::clone(&database),
        factory_address,
        event_sender.clone(),
        config.ethereum.poll_interval,
        config.ethereum.start_block,
    );

    tokio::spawn(async move {
        if let Err(e) = event_listener.start_monitoring().await {
            tracing::error!("Event listener error: {}", e);
        }
    });

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
