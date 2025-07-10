use crate::database::operations::EventOperations;
use crate::database::Database;
use anyhow::Result;
use ethers::{
    providers::{Http, Middleware, Provider},
    types::Address,
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
pub struct BaseEventListener {
    pub provider: Arc<Provider<Http>>,
    pub database: Arc<Database>,
    pub chain_id: u64,
    pub event_sender: broadcast::Sender<String>,
    pub poll_interval: Duration,
    pub last_processed_block: u64,
    pub start_block: u64,
    pub block_batch_size: u64,
    pub event_type: String, // 新增：事件类型标识
}

impl BaseEventListener {
    pub fn new(
        provider: Arc<Provider<Http>>,
        database: Arc<Database>,
        chain_id: u64,
        event_sender: broadcast::Sender<String>,
        poll_interval: u64,
        start_block: u64,
        block_batch_size: u64,
        event_type: String, // 新增参数
    ) -> Self {
        Self {
            provider,
            database,
            chain_id,
            event_sender,
            poll_interval: Duration::from_secs(poll_interval),
            last_processed_block: 0,
            start_block,
            block_batch_size,
            event_type,
        }
    }

    pub async fn initialize_last_processed_block(&mut self) -> Result<()> {
        // Initialize last processed block record if not exists
        EventOperations::initialize_last_processed_block(
            self.database.pool(),
            self.chain_id as i32,
            &self.event_type,
            self.start_block,
        )
        .await?;

        // Get last processed block from database
        self.last_processed_block = EventOperations::get_last_processed_block(
            self.database.pool(),
            self.chain_id as i32,
            &self.event_type,
        )
        .await?;

        if self.last_processed_block == 0 {
            self.last_processed_block = self.start_block;
            tracing::info!(
                "📍 链 {} ({}): 使用配置的起始区块: {}",
                self.chain_id,
                self.event_type,
                self.start_block
            );
        } else {
            tracing::info!(
                "📍 链 {} ({}): 从数据库恢复，上次处理到区块: {}",
                self.chain_id,
                self.event_type,
                self.last_processed_block
            );
        }

        Ok(())
    }

    pub async fn update_last_processed_block(&mut self, block_number: u64) -> Result<()> {
        self.last_processed_block = block_number;
        EventOperations::update_last_processed_block(
            self.database.pool(),
            self.chain_id as i32,
            &self.event_type,
            block_number,
        )
        .await
    }

    pub async fn get_current_block_range(&self) -> Result<Option<(u64, u64)>> {
        let latest_block = self.provider.get_block_number().await?.as_u64();

        if latest_block <= self.last_processed_block {
            return Ok(None);
        }

        let from_block = self.last_processed_block + 1;
        let to_block = std::cmp::min(from_block + self.block_batch_size - 1, latest_block);

        Ok(Some((from_block, to_block)))
    }

    pub async fn sleep_poll_interval(&self) {
        sleep(self.poll_interval).await;
    }
}
