use super::base_listener::BaseEventListener;
use crate::types::*;
use crate::database::operations::EVENT_TYPE_SWAP;
use anyhow::Result;
use chrono::{DateTime, Utc};
use ethers::{
    contract::{abigen, EthLogDecode},
    core::abi::RawLog,
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, H256, U256},
    utils::keccak256,
};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

abigen!(
    UniswapV2Pair,
    r#"[
        event Mint(address indexed sender, uint256 amount0, uint256 amount1)
        event Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)
        event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
    ]"#
);

pub struct SwapEventListener {
    base: BaseEventListener,
}

impl SwapEventListener {
    pub fn new(
        provider: Arc<Provider<Http>>,
        database: Arc<crate::database::Database>,
        chain_id: u64,
        event_sender: broadcast::Sender<String>,
        poll_interval: u64,
        start_block: u64,
        block_batch_size: u64,
    ) -> Self {
        Self {
            base: BaseEventListener::new(
                provider,
                database,
                chain_id,
                event_sender,
                poll_interval,
                start_block,
                block_batch_size,
                EVENT_TYPE_SWAP.to_string(),  // 使用交换事件类型
            ),
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("🚀 启动链 {} 的交换事件监控服务...", self.base.chain_id);
        info!("📊 区块批次大小: {}", self.base.block_batch_size);

        self.base.initialize_last_processed_block().await?;

        let latest_block = self.base.provider.get_block_number().await?.as_u64();
        info!("🔗 链 {} (交换): 当前最新区块: {}", self.base.chain_id, latest_block);

        if self.base.last_processed_block >= latest_block {
            info!("✅ 链 {} (交换): 已处理到最新区块，等待新区块...", self.base.chain_id);
        } else {
            let blocks_behind = latest_block - self.base.last_processed_block;
            info!(
                "⏳ 链 {} (交换): 需要处理 {} 个区块 (从 {} 到 {})",
                self.base.chain_id,
                blocks_behind,
                self.base.last_processed_block + 1,
                latest_block
            );
        }

        loop {
            if let Err(e) = self.poll_pair_events().await {
                error!("❌ 链 {} (交换): 轮询交换事件时出错: {}", self.base.chain_id, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }

            self.base.sleep_poll_interval().await;
        }
    }

    async fn poll_pair_events(&mut self) -> Result<()> {
        if let Some((from_block, to_block)) = self.base.get_current_block_range().await? {
            // Load existing pairs
            let pairs = crate::database::operations::get_all_pairs(
                self.base.database.pool(),
                Some(self.base.chain_id as i32),
            )
            .await?;

            let pair_addresses: Vec<Address> = pairs
                .iter()
                .filter_map(|p| {
                    match p.address.parse::<Address>() {
                        Ok(addr) => Some(addr),
                        Err(e) => {
                            warn!("链 {} (交换): 无法解析交易对地址 '{}': {}", self.base.chain_id, p.address, e);
                            None
                        }
                    }
                })
                .collect();

            if pair_addresses.is_empty() {
                debug!("📭 链 {} (交换): 没有交易对需要监控", self.base.chain_id);
                // 即使没有交易对，也要更新处理进度
                self.base.update_last_processed_block(to_block).await?;
                return Ok(());
            }

            info!(
                "💱 链 {} (交换): 开始查询 {} 个交易对的事件 (区块 {}-{})",
                self.base.chain_id,
                pair_addresses.len(),
                from_block,
                to_block
            );

            let mut total_events = 0;
            let mut failed_pairs = 0;
            let mut successful_pairs = 0;

            // Process each pair individually to avoid RPC limitations
            for (index, &pair_address) in pair_addresses.iter().enumerate() {
                match self.process_pair_events(pair_address, from_block, to_block).await {
                    Ok(event_count) => {
                        total_events += event_count;
                        successful_pairs += 1;
                        if event_count > 0 {
                            debug!(
                                "💱 链 {} (交换): 交易对 0x{:x} 处理了 {} 个事件",
                                self.base.chain_id, pair_address, event_count
                            );
                        }
                    }
                    Err(e) => {
                        if !e.to_string().contains("null") {
                            error!(
                                "❌ 链 {} (交换): 处理交易对 0x{:x} 事件失败: {}",
                                self.base.chain_id, pair_address, e
                            );
                            failed_pairs += 1;
                        } else {
                            successful_pairs += 1; // null 响应视为成功（无事件）
                        }
                    }
                }

                // Add small delay to avoid overwhelming RPC
                if index < pair_addresses.len() - 1 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }

            info!(
                "📊 链 {} (交换): 交易对事件处理总结 - 成功: {}, 失败: {}, 总事件: {}",
                self.base.chain_id, successful_pairs, failed_pairs, total_events
            );

            // 更新处理进度
            self.base.update_last_processed_block(to_block).await?;
        }

        Ok(())
    }

    async fn process_pair_events(
        &self,
        pair_address: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<u32> {
        let filter = Filter::new()
            .address(pair_address)
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()));

        let logs = self.base.provider.get_logs(&filter).await?;

        if logs.is_empty() {
            return Ok(0);
        }

        let mut event_count = 0;
        for log in logs {
            if let Err(e) = self.handle_pair_event(log).await {
                warn!(
                    "⚠️ 链 {} (交换): 处理交易对 0x{:x} 事件失败: {}",
                    self.base.chain_id, pair_address, e
                );
            } else {
                event_count += 1;
            }
        }

        Ok(event_count)
    }

    async fn handle_pair_event(&self, log: Log) -> Result<()> {
        let block_number = log.block_number.unwrap();
        let block_number_hex = format!("0x{:x}", block_number);
        let raw_block: serde_json::Value = self.base
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

        let event_signature = &log.topics[0];
        let swap_signature = H256::from(keccak256("Swap(address,uint256,uint256,uint256,uint256,address)"));
        let mint_signature = H256::from(keccak256("Mint(address,uint256,uint256)"));
        let burn_signature = H256::from(keccak256("Burn(address,uint256,uint256,address)"));

        if *event_signature == swap_signature {
            self.handle_swap_event(log, timestamp).await?;
        } else if *event_signature == mint_signature {
            self.handle_mint_event(log, timestamp).await?;
        } else if *event_signature == burn_signature {
            self.handle_burn_event(log, timestamp).await?;
        } else {
            debug!(
                "❓ 链 {} (交换): 未知事件类型 - 交易对: 0x{:x}, 签名: 0x{}",
                self.base.chain_id,
                log.address,
                hex::encode(event_signature.as_bytes())
            );
        }

        Ok(())
    }

    async fn handle_swap_event(&self, log: Log, timestamp: DateTime<Utc>) -> Result<()> {
        match SwapFilter::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.0.to_vec(),
        }) {
            Ok(event) => {
                let swap_event = SwapEvent {
                    id: Uuid::new_v4(),
                    chain_id: self.base.chain_id as i32,
                    pair_address: format!("0x{:x}", log.address),
                    sender: format!("0x{:x}", event.sender),
                    amount0_in: Decimal::from(event.amount_0_in.as_u128()),
                    amount1_in: Decimal::from(event.amount_1_in.as_u128()),
                    amount0_out: Decimal::from(event.amount_0_out.as_u128()),
                    amount1_out: Decimal::from(event.amount_1_out.as_u128()),
                    to_address: format!("0x{:x}", event.to),
                    block_number: log.block_number.unwrap().as_u64() as i64,
                    transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
                    log_index: log.log_index.unwrap().as_u32() as i32,
                    timestamp,
                };

                crate::database::operations::insert_swap_event(self.base.database.pool(), &swap_event).await?;
                let _ = self.base.event_sender.send(serde_json::to_string(&swap_event)?);

                debug!(
                    "💱 链 {} (交换): Swap事件已保存 - 交易对: {} (区块: {})",
                    self.base.chain_id, swap_event.pair_address, swap_event.block_number
                );
            }
            Err(e) => {
                // Try manual parsing as fallback
                match self.parse_swap_event_manually(&log) {
                    Ok((sender, amount0_in, amount1_in, amount0_out, amount1_out, to)) => {
                        let swap_event = SwapEvent {
                            id: Uuid::new_v4(),
                            chain_id: self.base.chain_id as i32,
                            pair_address: format!("0x{:x}", log.address),
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

                        crate::database::operations::insert_swap_event(self.base.database.pool(), &swap_event).await?;
                        let _ = self.base.event_sender.send(serde_json::to_string(&swap_event)?);

                        debug!(
                            "💱 链 {} (交换): Swap事件已保存(手动解析) - 交易对: {} (区块: {})",
                            self.base.chain_id, swap_event.pair_address, swap_event.block_number
                        );
                    }
                    Err(_) => {
                        warn!(
                            "⚠️ 链 {} (交换): Swap事件解析失败: {}",
                            self.base.chain_id, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_mint_event(&self, log: Log, timestamp: DateTime<Utc>) -> Result<()> {
        let event = MintFilter::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.0.to_vec(),
        })?;

        let mint_event = MintEvent {
            id: Uuid::new_v4(),
            chain_id: self.base.chain_id as i32,
            pair_address: format!("0x{:x}", log.address),
            sender: format!("0x{:x}", event.sender),
            amount0: Decimal::from(event.amount_0.as_u128()),
            amount1: Decimal::from(event.amount_1.as_u128()),
            block_number: log.block_number.unwrap().as_u64() as i64,
            transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
            log_index: log.log_index.unwrap().as_u32() as i32,
            timestamp,
        };

        crate::database::operations::insert_mint_event(self.base.database.pool(), &mint_event).await?;
        let _ = self.base.event_sender.send(serde_json::to_string(&mint_event)?);

        debug!(
            "🌱 链 {} (交换): Mint事件已保存 - 交易对: {} (区块: {})",
            self.base.chain_id, mint_event.pair_address, mint_event.block_number
        );

        Ok(())
    }

    async fn handle_burn_event(&self, log: Log, timestamp: DateTime<Utc>) -> Result<()> {
        let event = BurnFilter::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.0.to_vec(),
        })?;

        let burn_event = BurnEvent {
            id: Uuid::new_v4(),
            chain_id: self.base.chain_id as i32,
            pair_address: format!("0x{:x}", log.address),
            sender: format!("0x{:x}", event.sender),
            amount0: Decimal::from(event.amount_0.as_u128()),
            amount1: Decimal::from(event.amount_1.as_u128()),
            to_address: format!("0x{:x}", event.to),
            block_number: log.block_number.unwrap().as_u64() as i64,
            transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
            log_index: log.log_index.unwrap().as_u32() as i32,
            timestamp,
        };

        crate::database::operations::insert_burn_event(self.base.database.pool(), &burn_event).await?;
        let _ = self.base.event_sender.send(serde_json::to_string(&burn_event)?);

        debug!(
            "🔥 链 {} (交换): Burn事件已保存 - 交易对: {} (区块: {})",
            self.base.chain_id, burn_event.pair_address, burn_event.block_number
        );

        Ok(())
    }

    fn parse_swap_event_manually(&self, log: &Log) -> Result<(Address, U256, U256, U256, U256, Address)> {
        if log.topics.len() < 3 {
            return Err(anyhow::anyhow!("Swap 事件 topics 数量不足"));
        }

        let sender = Address::from(log.topics[1]);
        let to = Address::from(log.topics[2]);

        if log.data.0.len() < 128 {
            return Err(anyhow::anyhow!("Swap 事件 data 长度不足"));
        }

        let amount0_in = U256::from_big_endian(&log.data.0[0..32]);
        let amount1_in = U256::from_big_endian(&log.data.0[32..64]);
        let amount0_out = U256::from_big_endian(&log.data.0[64..96]);
        let amount1_out = U256::from_big_endian(&log.data.0[96..128]);

        Ok((sender, amount0_in, amount1_in, amount0_out, amount1_out, to))
    }
}
