#![allow(unused_variables)]  // 忽略未使用变量的警告
#![allow(dead_code)]        // 忽略未使用代码的警告
use uniswap_monitor::{Config, services::EventService};
use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber;

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

    // Create and start event service
    let event_service = EventService::new(config).await?;
    event_service.start().await?;

    Ok(())
}
