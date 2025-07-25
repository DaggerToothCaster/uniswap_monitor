// This is now just a wrapper that calls the combined service
#![allow(unused_variables)]  // 忽略未使用变量的警告
#![allow(warnings)]

use anyhow::Result;
use config::Config;
use tracing::{info, Level};
use tracing_subscriber;

mod config;
mod database;
mod event_listener;
mod services;
mod types;
mod api;


use services::{EventService,ApiService};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("🔧 启动UniswapV2监控服务 (默认组合模式)...");
    info!("💡 提示: 可以使用以下命令分别启动服务:");
    info!("   - 仅事件监听: cargo run --bin event-service");
    info!("   - 仅API服务: cargo run --bin api-service");
    info!("   - 组合服务: cargo run --bin combined-service");

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
