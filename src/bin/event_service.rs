
#![allow(warnings)]
use uniswap_monitor::{Config, services::{EventService,PriceService}};
use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🔧 启动独立事件监听服务...");

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded successfully");

    // Create database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database.url)
        .await?;

    // Create and start event service
    let event_service = EventService::new(config).await?;
    event_service.start().await?;

    // Create price service
    let price_service = PriceService::new(pool);
    let price_handle = tokio::spawn(async move {
        if let Err(e) = price_service.start().await {
            tracing::error!("Price service error: {}", e);
        }
    });
    
    Ok(())
}
