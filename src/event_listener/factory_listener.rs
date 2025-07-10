use super::base_listener::BaseEventListener;
use crate::types::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use ethers::{
    contract::{abigen, EthLogDecode},
    core::abi::RawLog,
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, U256},
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::database::operations::EVENT_TYPE_FACTORY;
use crate::database::operations::{EventOperations, TradingOperations};

abigen!(
    UniswapV2Factory,
    r#"[
        event PairCreated(address indexed token0, address indexed token1, address pair, uint256)
    ]"#
);

abigen!(
    ERC20Fixed,
    r#"[
        function symbol() external view returns (string)
        function name() external view returns (string)
        function decimals() external view returns (uint8)
    ]"#
);

pub struct FactoryEventListener {
    base: BaseEventListener,
    factory_address: Address,
}

impl FactoryEventListener {
    pub fn new(
        provider: Arc<Provider<Http>>,
        database: Arc<crate::database::Database>,
        chain_id: u64,
        factory_address: Address,
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
                EVENT_TYPE_FACTORY.to_string(), // 使用工厂事件类型
            ),
            factory_address,
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("🚀 启动链 {} 的工厂事件监控服务...", self.base.chain_id);
        info!("📊 区块批次大小: {}", self.base.block_batch_size);

        self.base.initialize_last_processed_block().await?;

        let latest_block = self.base.provider.get_block_number().await?.as_u64();
        info!(
            "🔗 链 {} (工厂): 当前最新区块: {}",
            self.base.chain_id, latest_block
        );

        if self.base.last_processed_block >= latest_block {
            info!(
                "✅ 链 {} (工厂): 已处理到最新区块，等待新区块...",
                self.base.chain_id
            );
        } else {
            let blocks_behind = latest_block - self.base.last_processed_block;
            info!(
                "⏳ 链 {} (工厂): 需要处理 {} 个区块 (从 {} 到 {})",
                self.base.chain_id,
                blocks_behind,
                self.base.last_processed_block + 1,
                latest_block
            );
        }

        loop {
            if let Err(e) = self.poll_factory_events().await {
                error!(
                    "❌ 链 {} (工厂): 轮询工厂事件时出错: {}",
                    self.base.chain_id, e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }

            self.base.sleep_poll_interval().await;
        }
    }

    async fn poll_factory_events(&mut self) -> Result<()> {
        if let Some((from_block, to_block)) = self.base.get_current_block_range().await? {
            info!(
                "🏭 链 {} (工厂): 处理工厂事件 - 区块: {}-{} (共 {} 个区块)",
                self.base.chain_id,
                from_block,
                to_block,
                to_block - from_block + 1
            );

            let filter = Filter::new()
                .address(self.factory_address)
                .from_block(BlockNumber::Number(from_block.into()))
                .to_block(BlockNumber::Number(to_block.into()))
                .event("PairCreated(address,address,address,uint256)");

            let logs = match self.base.provider.get_logs(&filter).await {
                Ok(logs) => {
                    debug!(
                        "🔍 链 {} (工厂): 区块 {}-{} 获取到 {} 个工厂事件",
                        self.base.chain_id,
                        from_block,
                        to_block,
                        logs.len()
                    );
                    logs
                }
                Err(e) => {
                    if e.to_string().to_lowercase().contains("null") {
                        debug!(
                            "📭 链 {} (工厂): 区块 {}-{} 返回空日志，视为无事件",
                            self.base.chain_id, from_block, to_block
                        );
                        self.base.update_last_processed_block(to_block).await?;
                        return Ok(());
                    } else {
                        error!(
                            "❌ 链 {} (工厂): 获取工厂事件失败: {}",
                            self.base.chain_id, e
                        );
                        return Err(e.into());
                    }
                }
            };

            // 如果日志为空，直接更新区块并返回
            if logs.is_empty() {
                debug!(
                    "📭 链 {} (工厂): 区块 {}-{} 中没有发现工厂事件",
                    self.base.chain_id, from_block, to_block
                );
                self.base.update_last_processed_block(to_block).await?;
                return Ok(());
            }

            // 处理有效日志
            info!(
                "🏭 链 {} (工厂): 发现 {} 个新交易对创建事件",
                self.base.chain_id,
                logs.len()
            );

            let mut processed = 0;
            let mut failed = 0;

            for (index, log) in logs.iter().enumerate() {
                if let Err(e) = self.handle_pair_created_event(log.clone()).await {
                    error!(
                        "❌ 链 {} (工厂): 处理第 {} 个PairCreated事件失败: {}",
                        self.base.chain_id,
                        index + 1,
                        e
                    );
                    failed += 1;
                } else {
                    processed += 1;
                }
            }

            info!(
                "📊 链 {} (工厂): 工厂事件处理总结 - 成功: {}, 失败: {}",
                self.base.chain_id, processed, failed
            );

            // 处理完成后更新最后处理的区块
            self.base.update_last_processed_block(to_block).await?;
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
            .base
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

        info!("🔍 链 {} (工厂): 读取 token 信息...", self.base.chain_id);
        let (token0_symbol, token0_name, token0_decimals) =
            self.get_token_info(event.token_0).await;
        let (token1_symbol, token1_name, token1_decimals) =
            self.get_token_info(event.token_1).await;

        let pair = TradingPair {
            id: Uuid::new_v4(),
            chain_id: self.base.chain_id as i32,
            address: format!("0x{:x}", event.pair),
            token0: format!("0x{:x}", event.token_0),
            token1: format!("0x{:x}", event.token_1),
            token0_symbol,
            token1_symbol,
            token0_decimals,
            token1_decimals,
            token0_name,
            token1_name,
            created_at: timestamp,
            block_number: log.block_number.unwrap().as_u64() as i64,
            transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
        };

        TradingOperations::insert_trading_pair(self.base.database.pool(), &pair).await?;

        let _ = self.base.event_sender.send(serde_json::to_string(&pair)?);

        info!(
            "🎉 链 {} (工厂): 新交易对创建 - {} (区块: {})",
            self.base.chain_id, pair.address, pair.block_number
        );
        info!(
            "   Token0: {} ({}) | Token1: {} ({})",
            pair.token0,
            pair.token0_symbol.as_deref().unwrap_or("Unknown"),
            pair.token1,
            pair.token1_symbol.as_deref().unwrap_or("Unknown")
        );

        Ok(())
    }

    async fn get_token_info(
        &self,
        token_address: Address,
    ) -> (Option<String>, Option<String>, Option<i32>) {
        let contract = ERC20Fixed::new(token_address, Arc::clone(&self.base.provider));

        let decimals = match contract.decimals().call().await {
            Ok(d) => Some(d as i32),
            Err(e) => {
                warn!(
                    "Failed to get decimals for token 0x{:x}: {}",
                    token_address, e
                );
                None
            }
        };

        let symbol = match contract.symbol().call().await {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(
                    "Failed to get symbol for token 0x{:x}: {}",
                    token_address, e
                );
                None
            }
        };

        let name = match contract.name().call().await {
            Ok(n) => Some(n),
            Err(e) => {
                warn!("Failed to get name for token 0x{:x}: {}", token_address, e);
                None
            }
        };

        (symbol, name, decimals)
    }
}
