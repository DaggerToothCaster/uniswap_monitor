#![allow(unused_variables)] // 忽略未使用变量的警告
#![allow(dead_code)] // 忽略未使用代码的警告

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
use ethers::{
    providers::{Http, Provider},
    types::Address,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

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

    // 创建 providers 和 factory_addresses 映射
    let mut providers = HashMap::new();
    let mut factory_addresses = HashMap::new();

    // Start event listeners for each enabled chain
    for (chain_id, chain_config) in &config.chains {
        if !chain_config.enabled {
            info!(
                "Chain {} ({}) is disabled, skipping",
                chain_id, chain_config.name
            );
            continue;
        }

        info!(
            "Starting monitoring for chain {} ({})",
            chain_id, chain_config.name
        );

        let provider = Arc::new(Provider::<Http>::try_from(&chain_config.rpc_url)?);
        let factory_address: Address = chain_config.factory_address.parse()?;

        // 保存到映射中，供API使用
        providers.insert(*chain_id, Arc::clone(&provider));
        factory_addresses.insert(*chain_id, factory_address);

        // 👇 克隆必要字段，避免非 'static 引用
        let chain_id_cloned = *chain_id;
        let provider_cloned = Arc::clone(&provider);
        let database_cloned = Arc::clone(&database);
        let factory_address_cloned = factory_address;
        let event_sender_cloned = event_sender.clone();
        let poll_interval = chain_config.poll_interval;
        let start_block = chain_config.start_block;
        let factory_block_batch_size = chain_config.factory_block_batch_size;
        let pair_block_batch_size = chain_config.pair_block_batch_size;

        tokio::spawn(async move {
            let mut event_listener = EventListener::new(
                provider_cloned,
                database_cloned,
                chain_id_cloned,
                factory_address_cloned,
                event_sender_cloned,
                poll_interval,
                start_block,
                factory_block_batch_size,
                pair_block_batch_size,
            );

            if let Err(e) = event_listener.start_monitoring().await {
                tracing::error!("Event listener error for chain {}: {}", chain_id_cloned, e);
            }
        });
    }

    // Start API server
    let api_state = ApiState {
        database: Arc::clone(&database),
        event_sender,
        providers,
        factory_addresses,
    };

    let app = create_router(api_state);
    let listener =
        tokio::net::TcpListener::bind(format!("{}:{}", config.server.host, config.server.port))
            .await?;

    info!(
        "Server starting on {}:{}",
        config.server.host, config.server.port
    );
    axum::serve(listener, app).await?;

    Ok(())
}
