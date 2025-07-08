use uniswap_monitor::{Config, services::{EventService, ApiService}};
use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🔧 启动组合服务 (事件监听 + API)...");

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded successfully");

    // Create event service first to get the event sender
    let event_service = EventService::new(config.clone()).await?;
    let event_sender = event_service.get_event_sender();

    // Create API service with shared event sender
    let api_service = ApiService::new(config, Some(event_sender)).await?;

    // Start both services concurrently
    let event_handle = tokio::spawn(async move {
        if let Err(e) = event_service.start().await {
            tracing::error!("Event service error: {}", e);
        }
    });

    let api_handle = tokio::spawn(async move {
        if let Err(e) = api_service.start().await {
            tracing::error!("API service error: {}", e);
        }
    });

    // Wait for both services
    tokio::try_join!(event_handle, api_handle)?;

    Ok(())
}
