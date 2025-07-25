pub mod base_listener;
pub mod factory_listener;
pub mod swap_listener;

pub use base_listener::BaseEventListener;
pub use factory_listener::FactoryEventListener;
pub use swap_listener::SwapEventListener;

use crate::config::ChainConfig;
use crate::database::Database;
use anyhow::Result;
use ethers::{
    providers::{Http, Provider},
    types::Address,
};
use std::sync::Arc;
use tokio::sync::broadcast;
#[derive(Clone)]
pub struct EventListenerManager {
    database: Arc<Database>,
    event_sender: broadcast::Sender<String>,
}

impl EventListenerManager {
    pub fn new(database: Arc<Database>, event_sender: broadcast::Sender<String>) -> Self {
        Self {
            database,
            event_sender,
        }
    }

    pub async fn start_chain_listeners(&self, chain_id: u64, config: &ChainConfig) -> Result<()> {
        let provider = Arc::new(Provider::<Http>::try_from(&config.rpc_url)?);
        let factory_address: Address = config.factory_address.parse()?;

        // 启动工厂事件监听器
        let factory_provider = Arc::clone(&provider);
        let factory_database = Arc::clone(&self.database);
        let factory_sender = self.event_sender.clone();
        let factory_config = config.clone();

        let factory_handle = tokio::spawn(async move {
            let mut factory_listener = FactoryEventListener::new(
                factory_provider,
                factory_database,
                chain_id,
                factory_address,
                factory_sender,
                factory_config.poll_interval,
                factory_config.start_block,
                factory_config.block_batch_size,
            );

            if let Err(e) = factory_listener.start_monitoring().await {
                tracing::error!("链 {} 工厂事件监听器错误: {}", chain_id, e);
            }
        });

        // 启动交换事件监听器
        let swap_provider = Arc::clone(&provider);
        let swap_database = Arc::clone(&self.database);
        let swap_sender = self.event_sender.clone();
        let swap_config = config.clone();

        let swap_handle = tokio::spawn(async move {
            let mut swap_listener = SwapEventListener::new(
                swap_provider,
                swap_database,
                chain_id,
                swap_sender,
                swap_config.poll_interval,
                swap_config.start_block,
                swap_config.block_batch_size,
            );

            if let Err(e) = swap_listener.start_monitoring().await {
                tracing::error!("链 {} 交换事件监听器错误: {}", chain_id, e);
            }
        });

        // 等待两个监听器
        tokio::try_join!(factory_handle, swap_handle)?;

        Ok(())
    }
}
