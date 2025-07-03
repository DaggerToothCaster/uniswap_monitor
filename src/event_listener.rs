use crate::database::Database;
use crate::models::*;
use anyhow::{Result,anyhow};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use ethers::{
    abi::RawLog,
    contract::{abigen, Contract, EthLogDecode},
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, U256, Block, TxHash},
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

abigen!(
    UniswapV2Factory,
    r#"[
        event PairCreated(address indexed token0, address indexed token1, address pair, uint256)
    ]"#
);

abigen!(
    UniswapV2Pair,
    r#"[
        event Mint(address indexed sender, uint256 amount0, uint256 amount1)
        event Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)
        event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
    ]"#
);

pub struct EventListener {
    provider: Arc<Provider<Http>>,
    database: Arc<Database>,
    factory_address: Address,
    event_sender: broadcast::Sender<String>,
    poll_interval: Duration,
    last_processed_block: u64,
}

impl EventListener {
    pub fn new(
        provider: Arc<Provider<Http>>,
        database: Arc<Database>,
        factory_address: Address,
        event_sender: broadcast::Sender<String>,
        poll_interval: u64,
        start_block: u64,
    ) -> Self {
        Self {
            provider,
            database,
            factory_address,
            event_sender,
            poll_interval: Duration::from_secs(poll_interval),
            last_processed_block: start_block,
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("Starting event monitoring with polling...");

        // Get the last processed block from database
        if let Some(last_block) = self.database.get_last_processed_block().await? {
            self.last_processed_block = last_block;
            info!("Resuming from block: {}", last_block);
        } else {
            info!(
                "Starting from configured block: {}",
                self.last_processed_block
            );
        }

        // Load existing pairs and start monitoring them
        let pairs = self.database.get_all_pairs().await?;
        let pair_addresses: Vec<Address> = pairs
            .iter()
            .filter_map(|p| p.address.parse().ok())
            .collect();

        info!("Monitoring {} existing pairs", pair_addresses.len());

        loop {
            if let Err(e) = self.poll_events(&pair_addresses).await {
                error!("Error polling events: {}", e);
                // Wait a bit before retrying
                sleep(Duration::from_secs(5)).await;
            }

            sleep(self.poll_interval).await;
        }
    }

    async fn poll_events(&mut self, existing_pairs: &[Address]) -> Result<()> {
        let latest_block = match self.provider.get_block_number().await {
            Ok(num) => num.as_u64(),
            Err(e) => {
                error!("Error getting latest block: {}", e);
                return Ok(()); // 跳过这次轮询
            }
        };

        if latest_block <= self.last_processed_block {
            return Ok(());
        }

        let from_block = self.last_processed_block + 1;
        let to_block = std::cmp::min(from_block + 1000, latest_block);

        info!("Processing blocks {} to {}", from_block, to_block);

        // 处理工厂事件
        if let Err(e) = self.poll_factory_events(from_block, to_block).await {
            error!("Error polling factory events: {}", e);
        }

        // 处理配对事件
        if let Err(e) = self
            .poll_pair_events(existing_pairs, from_block, to_block)
            .await
        {
            error!("Error polling pair events: {}", e);
        }

        self.last_processed_block = to_block;
        Ok(())
    }

    async fn poll_factory_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        let filter = Filter::new()
            .address(self.factory_address)
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .event("PairCreated(address,address,address,uint256)");

        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(e) => {
                warn!("No factory events found or error fetching: {}", e);
                return Ok(());
            }
        };

        for log in logs {
            if let Err(e) = self.handle_pair_created_event(log).await {
                error!("Error handling PairCreated event: {}", e);
            }
        }

        Ok(())
    }

    async fn poll_pair_events(
        &self,
        pair_addresses: &[Address],
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        if pair_addresses.is_empty() {
            return Ok(());
        }

        let filter = Filter::new()
            .address(pair_addresses.to_vec())
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()));

        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(e) => {
                warn!("No pair events found or error fetching: {}", e);
                return Ok(());
            }
        };

        for log in logs {
            if let Err(e) = self.handle_pair_event(log).await {
                error!("Error handling pair event: {}", e);
            }
        }

        Ok(())
    }

    async fn handle_pair_created_event(&self, log: Log) -> Result<()> {
        // 1. 验证日志数据完整性
        if log.topics.len() != 3 {
            return Err(anyhow::anyhow!(
                "Invalid PairCreated event: expected 3 topics (got {})",
                log.topics.len()
            ));
        }

        // 2. 手动解码事件数据（避免ABI解码器问题）
        let token0 = Address::from_slice(&log.topics[1].as_bytes()[12..]);
        let token1 = Address::from_slice(&log.topics[2].as_bytes()[12..]);
        let pair = Address::from_slice(&log.data.to_vec()[12..32]);

        // 3. 获取区块信息（带重试机制）
        let block_number = log.block_number.ok_or(anyhow!("Missing block number"))?;
        let block = self.get_block_with_retry(BlockNumber::Number(block_number)).await?;
        let timestamp = DateTime::from_timestamp(block.timestamp.as_u64() as i64, 0)
            .unwrap_or_else(|| Utc::now());

        // 4. 构建交易对记录
        let pair_record = TradingPair {
            id: Uuid::new_v4(),
            address: format!("{:?}", pair),
            token0: format!("{:?}", token0),
            token1: format!("{:?}", token1),
            token0_symbol: None,
            token1_symbol: None,
            token0_decimals: None,
            token1_decimals: None,
            created_at: timestamp,
            block_number: block_number.as_u64() as i64,
            transaction_hash: format!(
                "{:?}",
                log.transaction_hash.ok_or(anyhow!("Missing tx hash"))?
            ),
        };

        // 5. 保存并通知
        self.database.insert_trading_pair(&pair_record).await?;
        let _ = self.event_sender.send(serde_json::to_string(&pair_record)?);

        info!(
            "New pair created: {}/{} at {}",
            pair_record.token0, pair_record.token1, pair_record.address
        );
        Ok(())
    }

    async fn get_block_with_retry(&self, block_number: BlockNumber) -> Result<Block<TxHash>> {
        const MAX_RETRIES: usize = 3;
        let mut last_error = None;

        for _ in 0..MAX_RETRIES {
            match self.provider.get_block(block_number).await {
                Ok(Some(block)) => return Ok(block),
                Ok(None) => return Err(anyhow!("Block not found")),
                Err(e) => {
                    last_error = Some(e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Err(anyhow!("Failed after retries: {:?}", last_error))
    }
    async fn handle_pair_event(&self, log: Log) -> Result<()> {
        let block = self.provider.get_block(log.block_number.unwrap()).await?;
        let timestamp = DateTime::from_timestamp(block.unwrap().timestamp.as_u64() as i64, 0)
            .unwrap_or_else(|| Utc::now());

        let pair_address = log.address;
        let event_signature = &log.topics[0];

        // Swap event signature: keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
        let swap_signature = [
            0xd7, 0x8a, 0xd9, 0x5f, 0xa4, 0x6c, 0x99, 0x4b, 0x6e, 0x6f, 0x0d, 0x4a, 0xaa, 0x7c,
            0xe5, 0xbd, 0x1e, 0xdd, 0x3e, 0x86, 0xef, 0x3e, 0x7e, 0x93, 0xb2, 0xa0, 0x8c, 0x5d,
            0x0e, 0x57, 0x9b, 0x9b,
        ];

        if event_signature.as_bytes() == swap_signature {
            // Decode swap event manually since we can't use the contract decoder easily
            let sender = Address::from_slice(&log.topics[1][12..]);
            let to = Address::from_slice(&log.topics[2][12..]);

            // Decode data (4 uint256 values)
            let data = &log.data;
            let amount0_in = U256::from_big_endian(&data[0..32]);
            let amount1_in = U256::from_big_endian(&data[32..64]);
            let amount0_out = U256::from_big_endian(&data[64..96]);
            let amount1_out = U256::from_big_endian(&data[96..128]);

            let swap_event = SwapEvent {
                id: Uuid::new_v4(),
                pair_address: format!("0x{:x}", pair_address),
                sender: format!("0x{:x}", sender),
                amount0_in: BigDecimal::from(amount0_in.as_u128()),
                amount1_in: BigDecimal::from(amount1_in.as_u128()),
                amount0_out: BigDecimal::from(amount0_out.as_u128()),
                amount1_out: BigDecimal::from(amount1_out.as_u128()),
                to_address: format!("0x{:x}", to),
                block_number: log.block_number.unwrap().as_u64() as i64,
                transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
                log_index: log.log_index.unwrap().as_u32() as i32,
                timestamp,
            };

            self.database.insert_swap_event(&swap_event).await?;
            let _ = self.event_sender.send(serde_json::to_string(&swap_event)?);

            info!("Swap event processed for pair: {}", swap_event.pair_address);
        }
        // Add similar handling for Mint and Burn events if needed

        Ok(())
    }
}
