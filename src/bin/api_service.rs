#![allow(unused_variables)]  // 忽略未使用变量的警告
#![allow(dead_code)]        // 忽略未使用代码的警告
use uniswap_monitor::services::{ApiService, EventService};
use anyhow::Result;
use uniswap_monitor::config::Config;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("🔧 启动独立API服务...");

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded successfully");

    // Create and start API service
    let api_service = ApiService::new(config, None).await?;
    api_service.start().await?;

    Ok(())
}
