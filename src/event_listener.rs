use crate::database::Database;
use crate::models::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use ethers::{
    contract::{abigen, Contract, EthLogDecode},
    core::abi::RawLog,
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, U256},
};
use rust_decimal::Decimal;
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

pub struct EventListener {
    provider: Arc<Provider<Http>>,
    database: Arc<Database>,
    chain_id: u64,
    factory_address: Address,
    event_sender: broadcast::Sender<String>,
    poll_interval: Duration,
    last_processed_block: u64,
}

impl EventListener {
    pub fn new(
        provider: Arc<Provider<Http>>,
        database: Arc<Database>,
        chain_id: u64,
        factory_address: Address,
        event_sender: broadcast::Sender<String>,
        poll_interval: u64,
        start_block: u64,
    ) -> Self {
        Self {
            provider,
            database,
            chain_id,
            factory_address,
            event_sender,
            poll_interval: Duration::from_secs(poll_interval),
            last_processed_block: start_block,
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!(
            "Starting event monitoring for chain {} with polling...",
            self.chain_id
        );

        // Get the last processed block from database
        if let Some(last_block) = self
            .database
            .get_last_processed_block(self.chain_id as i32)
            .await?
        {
            self.last_processed_block = last_block;
            info!(
                "Chain {}: Resuming from block: {}",
                self.chain_id, last_block
            );
        } else {
            info!(
                "Chain {}: Starting from configured block: {}",
                self.chain_id, self.last_processed_block
            );
        }

        // Load existing pairs and start monitoring them
        let pairs = self
            .database
            .get_all_pairs(Some(self.chain_id as i32))
            .await?;
        let pair_addresses: Vec<Address> = pairs
            .iter()
            .filter_map(|p| p.address.parse().ok())
            .collect();

        info!(
            "Chain {}: Monitoring {} existing pairs",
            self.chain_id,
            pair_addresses.len()
        );

        loop {
            if let Err(e) = self.poll_events(&pair_addresses).await {
                error!("Chain {}: Error polling events: {}", self.chain_id, e);
                // Wait a bit before retrying
                sleep(Duration::from_secs(5)).await;
            }

            sleep(self.poll_interval).await;
        }
    }

    async fn poll_events(&mut self, existing_pairs: &[Address]) -> Result<()> {
        let latest_block = self.provider.get_block_number().await?.as_u64();

        if latest_block <= self.last_processed_block {
            return Ok(());
        }

        let from_block = self.last_processed_block + 1;
        let to_block = std::cmp::min(from_block + 1000, latest_block);

        info!(
            "Chain {}: Processing blocks {} to {}",
            self.chain_id, from_block, to_block
        );

        // Poll for new pairs
        self.poll_factory_events(from_block, to_block).await?;

        // Poll for pair events
        self.poll_pair_events(existing_pairs, from_block, to_block)
            .await?;

        self.last_processed_block = to_block;
        Ok(())
    }

    async fn poll_factory_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        let filter = Filter::new()
            .address(self.factory_address)
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .event("PairCreated(address,address,address,uint256)");

        let logs = self.provider.get_logs(&filter).await?;
        // 打印logs
        for log in &logs {
            match serde_json::to_string_pretty(log) {
                Ok(json) => info!("Chain {}: PairCreated log:\n{}", self.chain_id, json),
                Err(e) => warn!(
                    "Chain {}: Failed to serialize log to JSON: {}",
                    self.chain_id, e
                ),
            }
        }

        for log in logs {
            if let Err(e) = self.handle_pair_created_event(log).await {
                error!(
                    "Chain {}: Error handling PairCreated event: {}",
                    self.chain_id, e
                );
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

        let logs = self.provider.get_logs(&filter).await?;

        for log in logs {
            if let Err(e) = self.handle_pair_event(log).await {
                error!("Chain {}: Error handling pair event: {}", self.chain_id, e);
            }
        }

        Ok(())
    }

    async fn handle_pair_created_event(&self, log: Log) -> Result<()> {
        let event = PairCreatedFilter::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.0.to_vec(),
        })?;

        let block_number = log.block_number.unwrap();
        let block_number_hex = format!("0x{:x}", block_number);

        let raw_block: serde_json::Value = self
            .provider
            .request(
                "eth_getBlockByNumber",
                serde_json::json!([block_number_hex, false]),
            )
            .await?;

        let timestamp_hex = raw_block["timestamp"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing timestamp field"))?;

        let timestamp_u64 = u64::from_str_radix(timestamp_hex.trim_start_matches("0x"), 16)?;
        let timestamp =
            DateTime::<Utc>::from_timestamp(timestamp_u64 as i64, 0).unwrap_or_else(|| Utc::now());

        let pair = TradingPair {
            id: Uuid::new_v4(),
            chain_id: self.chain_id as i32,
            address: format!("0x{:x}", event.pair),
            token0: format!("0x{:x}", event.token_0),
            token1: format!("0x{:x}", event.token_1),
            token0_symbol: None,
            token1_symbol: None,
            token0_decimals: None,
            token1_decimals: None,
            token0_name: None,
            token1_name: None,
            created_at: timestamp,
            block_number: log.block_number.unwrap().as_u64() as i64,
            transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
        };

        self.database.insert_trading_pair(&pair).await?;

        // Notify frontend about new pair
        let _ = self.event_sender.send(serde_json::to_string(&pair)?);

        info!(
            "Chain {}: New pair created: {} - {}/{}",
            self.chain_id, pair.address, pair.token0, pair.token1
        );

        Ok(())
    }

    async fn handle_pair_event(&self, log: Log) -> Result<()> {
        let block_number = log.block_number.unwrap();
        let block_number_hex = format!("0x{:x}", block_number);

        let raw_block: serde_json::Value = self
            .provider
            .request(
                "eth_getBlockByNumber",
                serde_json::json!([block_number_hex, false]),
            )
            .await?;

        let timestamp_hex = raw_block["timestamp"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing timestamp field"))?;

        let timestamp_u64 = u64::from_str_radix(timestamp_hex.trim_start_matches("0x"), 16)?;
        let timestamp =
            DateTime::<Utc>::from_timestamp(timestamp_u64 as i64, 0).unwrap_or_else(|| Utc::now());

        let pair_address = log.address;
        let event_signature = &log.topics[0];

        // Swap event signature: keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
        let swap_signature = [
            0xd7, 0x8a, 0xd9, 0x5f, 0xa4, 0x6c, 0x99, 0x4b, 0x6e, 0x6f, 0x0d, 0x4a, 0xaa, 0x7c,
            0xe5, 0xbd, 0x1e, 0xdd, 0x3e, 0x86, 0xef, 0x3e, 0x7e, 0x93, 0xb2, 0xa0, 0x8c, 0x5d,
            0x0e, 0x57, 0x9b, 0x9b,
        ];

        if event_signature.as_bytes() == swap_signature {
            let sender = Address::from_slice(&log.topics[1][12..]);
            let to = Address::from_slice(&log.topics[2][12..]);

            let data = &log.data;
            let amount0_in = U256::from_big_endian(&data[0..32]);
            let amount1_in = U256::from_big_endian(&data[32..64]);
            let amount0_out = U256::from_big_endian(&data[64..96]);
            let amount1_out = U256::from_big_endian(&data[96..128]);

            let swap_event = SwapEvent {
                id: Uuid::new_v4(),
                chain_id: self.chain_id as i32,
                pair_address: format!("0x{:x}", pair_address),
                sender: format!("0x{:x}", sender),
                amount0_in: Decimal::from(amount0_in.as_u128()),
                amount1_in: Decimal::from(amount1_in.as_u128()),
                amount0_out: Decimal::from(amount0_out.as_u128()),
                amount1_out: Decimal::from(amount1_out.as_u128()),
                to_address: format!("0x{:x}", to),
                block_number: log.block_number.unwrap().as_u64() as i64,
                transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
                log_index: log.log_index.unwrap().as_u32() as i32,
                timestamp,
            };

            self.database.insert_swap_event(&swap_event).await?;
            let _ = self.event_sender.send(serde_json::to_string(&swap_event)?);

            info!(
                "Chain {}: Swap event processed for pair: {}",
                self.chain_id, swap_event.pair_address
            );
        }

        Ok(())
    }
}
