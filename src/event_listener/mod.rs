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

        // Start factory event listener
        let mut factory_listener = FactoryEventListener::new(
            Arc::clone(&provider),
            Arc::clone(&self.database),
            chain_id,
            factory_address,
            self.event_sender.clone(),
            config.poll_interval,
            config.start_block,
            config.factory_block_batch_size,
            config.pair_block_batch_size,
        );

        let factory_handle = tokio::spawn(async move {
            if let Err(e) = factory_listener.start_monitoring().await {
                tracing::error!("Factory listener error for chain {}: {}", chain_id, e);
            }
        });

        // Start swap event listener
        let mut swap_listener = SwapEventListener::new(
            Arc::clone(&provider),
            Arc::clone(&self.database),
            chain_id,
            self.event_sender.clone(),
            config.poll_interval,
            config.start_block,
            config.pair_block_batch_size,
        );

        let swap_handle = tokio::spawn(async move {
            if let Err(e) = swap_listener.start_monitoring().await {
                tracing::error!("Swap listener error for chain {}: {}", chain_id, e);
            }
        });

        // Wait for both listeners (they run indefinitely)
        tokio::try_join!(factory_handle, swap_handle)?;

        Ok(())
    }
}
