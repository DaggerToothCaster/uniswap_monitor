use crate::database::Database;
use crate::models::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use ethers::types::Bytes;
use ethers::{
    contract::{abigen, Contract, EthLogDecode},
    core::abi::RawLog,
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, H256, U256},
};
use rust_decimal::Decimal;
use std::convert::TryInto;
use std::sync::Arc;
use std::u8;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
    chain_id: u64,
    factory_address: Address,
    event_sender: broadcast::Sender<String>,
    poll_interval: Duration,
    last_processed_block: u64,
    start_block: u64,
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
            last_processed_block: 0,
            start_block,
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("🚀 启动链 {} 的事件监控服务...", self.chain_id);

        // 初始化最后处理区块记录（如果不存在）
        self.database
            .initialize_last_processed_block(self.chain_id as i32, self.start_block)
            .await?;

        // 从数据库获取最后处理的区块
        self.last_processed_block = self
            .database
            .get_last_processed_block(self.chain_id as i32)
            .await?;

        // 如果数据库中的值为0，使用配置的起始区块
        if self.last_processed_block == 0 {
            self.last_processed_block = self.start_block;
            info!(
                "📍 链 {}: 使用配置的起始区块: {}",
                self.chain_id, self.start_block
            );
        } else {
            info!(
                "📍 链 {}: 从数据库恢复，上次处理到区块: {}",
                self.chain_id, self.last_processed_block
            );
        }

        // 获取当前最新区块
        let latest_block = self.provider.get_block_number().await?.as_u64();
        info!("🔗 链 {}: 当前最新区块: {}", self.chain_id, latest_block);

        if self.last_processed_block >= latest_block {
            info!("✅ 链 {}: 已处理到最新区块，等待新区块...", self.chain_id);
        } else {
            let blocks_behind = latest_block - self.last_processed_block;
            info!(
                "⏳ 链 {}: 需要处理 {} 个区块 (从 {} 到 {})",
                self.chain_id,
                blocks_behind,
                self.last_processed_block + 1,
                latest_block
            );
        }

        // 加载现有交易对
        let pairs = self
            .database
            .get_all_pairs(Some(self.chain_id as i32))
            .await?;
        let pair_addresses: Vec<Address> = pairs
            .iter()
            .filter_map(|p| p.address.parse().ok())
            .collect();

        info!(
            "📊 链 {}: 监控 {} 个现有交易对",
            self.chain_id,
            pair_addresses.len()
        );

        // 开始轮询循环
        loop {
            if let Err(e) = self.poll_events(&pair_addresses).await {
                error!("❌ 链 {}: 轮询事件时出错: {}", self.chain_id, e);
                // 等待一段时间后重试
                sleep(Duration::from_secs(5)).await;
            }

            sleep(self.poll_interval).await;
        }
    }

    // 新增：处理指定块范围的事件
    pub async fn process_block_range(&self, from_block: u64, to_block: u64) -> Result<()> {
        info!(
            "🔧 链 {}: 手动处理区块范围 {} 到 {}",
            self.chain_id, from_block, to_block
        );

        // 加载现有交易对
        let pairs = self
            .database
            .get_all_pairs(Some(self.chain_id as i32))
            .await?;
        let pair_addresses: Vec<Address> = pairs
            .iter()
            .filter_map(|p| p.address.parse().ok())
            .collect();

        // 分批处理，避免请求过大
        let batch_size = 1000u64;
        let mut current_from = from_block;

        while current_from <= to_block {
            let current_to = std::cmp::min(current_from + batch_size - 1, to_block);

            info!(
                "🔍 链 {}: 手动处理区块 {} 到 {}",
                self.chain_id, current_from, current_to
            );

            // 处理工厂事件
            if let Err(e) = self.poll_factory_events(current_from, current_to).await {
                error!("❌ 链 {}: 手动处理工厂事件失败: {}", self.chain_id, e);
                return Err(e);
            }

            // 处理交易对事件
            if let Err(e) = self
                .poll_pair_events(&pair_addresses, current_from, current_to)
                .await
            {
                error!("❌ 链 {}: 手动处理交易对事件失败: {}", self.chain_id, e);
                return Err(e);
            }

            current_from = current_to + 1;
        }

        info!("✅ 链 {}: 手动处理完成", self.chain_id);
        Ok(())
    }

    async fn poll_events(&mut self, existing_pairs: &[Address]) -> Result<()> {
        let latest_block = self.provider.get_block_number().await?.as_u64();

        // 如果没有新区块，直接返回
        if latest_block <= self.last_processed_block {
            debug!(
                "🔄 链 {}: 没有新区块，当前: {}, 最新: {}",
                self.chain_id, self.last_processed_block, latest_block
            );
            return Ok(());
        }

        let from_block = self.last_processed_block + 1;
        // 限制每次处理的区块数量，避免请求过大
        let to_block = std::cmp::min(from_block + 3000, latest_block);

        info!(
            "🔍 链 {}: 处理区块 {} 到 {} (共 {} 个区块)",
            self.chain_id,
            from_block,
            to_block,
            to_block - from_block + 1
        );

        // 轮询工厂合约的新交易对事件
        if let Err(e) = self.poll_factory_events(from_block, to_block).await {
            error!("❌ 链 {}: 处理工厂事件失败: {}", self.chain_id, e);
            return Err(e);
        }

        // 轮询现有交易对的事件
        if let Err(e) = self
            .poll_pair_events(existing_pairs, from_block, to_block)
            .await
        {
            error!("❌ 链 {}: 处理交易对事件失败: {}", self.chain_id, e);
            return Err(e);
        }

        // 更新最后处理的区块到数据库
        self.last_processed_block = to_block;
        if let Err(e) = self
            .database
            .update_last_processed_block(self.chain_id as i32, to_block)
            .await
        {
            error!("❌ 链 {}: 更新最后处理区块失败: {}", self.chain_id, e);
            return Err(e);
        }

        debug!("✅ 链 {}: 成功处理到区块 {}", self.chain_id, to_block);

        // 如果还有更多区块需要处理，显示进度
        if to_block < latest_block {
            let remaining = latest_block - to_block;
            info!(
                "📈 链 {}: 处理进度 - 剩余 {} 个区块",
                self.chain_id, remaining
            );
        }

        Ok(())
    }

    async fn poll_factory_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        let filter = Filter::new()
            .address(self.factory_address)
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .event("PairCreated(address,address,address,uint256)");

        let logs_opt = self.provider.get_logs(&filter).await.ok();
        let logs = match logs_opt {
            Some(logs) => logs,
            None => {
                warn!(
                    "Chain {}: No logs returned for PairCreated event (logs is null)",
                    self.chain_id
                );
                return Ok(());
            }
        };

        if !logs.is_empty() {
            info!(
                "🏭 链 {}: 发现 {} 个新交易对创建事件",
                self.chain_id,
                logs.len()
            );
        }

        // 打印logs（仅在debug模式下）
        for log in &logs {
            match serde_json::to_string_pretty(log) {
                Ok(json) => debug!("Chain {}: PairCreated log:\n{}", self.chain_id, json),
                Err(e) => warn!(
                    "Chain {}: Failed to serialize log to JSON: {}",
                    self.chain_id, e
                ),
            }
        }

        for (index, log) in logs.iter().enumerate() {
            if let Err(e) = self.handle_pair_created_event(log.clone()).await {
                error!(
                    "❌ 链 {}: 处理第 {} 个PairCreated事件失败: {}",
                    self.chain_id,
                    index + 1,
                    e
                );
                // 继续处理其他事件，不要因为一个事件失败就停止
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
            tracing::debug!("📭 链 {}: 没有交易对需要监控", self.chain_id);
            return Ok(());
        }

        // 分批处理交易对地址，避免请求过大
        const BATCH_SIZE: usize = 100;
        for chunk in pair_addresses.chunks(BATCH_SIZE) {
            let filter = Filter::new()
                .address(chunk.to_vec())
                .from_block(BlockNumber::Number(from_block.into()))
                .to_block(BlockNumber::Number(to_block.into()));

            let logs_opt = self.provider.get_logs(&filter).await.ok();
            let logs = match logs_opt {
                Some(logs) => {
                    // 按交易对统计事件数量
                    let mut event_counts: std::collections::HashMap<Address, usize> =
                        std::collections::HashMap::new();
                    for log in &logs {
                        *event_counts.entry(log.address).or_insert(0) += 1;
                    }

                    // 打印每个交易对的事件长度
                    for (pair, count) in event_counts {
                        tracing::info!(
                            "======= 链 {}: 交易对 0x{:x} 在区块 {}-{} 中获取到 {} 个事件",
                            self.chain_id,
                            pair,
                            from_block,
                            to_block,
                            count
                        );
                    }

                    logs
                }
                None => {
                    tracing::warn!(
                        "链 {}: 在区块 {}-{} 中没有获取到交易对事件",
                        self.chain_id,
                        from_block,
                        to_block
                    );
                    continue;
                }
            };

            if !logs.is_empty() {
                tracing::info!(
                    "💱 链 {}: 在 {} 个交易对中发现 {} 个事件 (区块 {}-{})",
                    self.chain_id,
                    chunk.len(),
                    logs.len(),
                    from_block,
                    to_block
                );
            }

            for (index, log) in logs.iter().enumerate() {
                if let Err(e) = self.handle_pair_event(log.clone()).await {
                    tracing::error!(
                        "❌ 链 {}: 处理第 {} 个交易对事件失败: {}",
                        self.chain_id,
                        index + 1,
                        e
                    );
                    // 继续处理其他事件
                }
            }
        }

        Ok(())
    }

    async fn get_token_info(
        &self,
        token_address: Address,
    ) -> (Option<String>, Option<String>, Option<i32>) {
        let contract = ERC20Fixed::new(token_address, Arc::clone(&self.provider));

        let decimals = match contract.decimals().call().await {
            Ok(d) => Some(d as i32),
            Err(e) => {
                error!(
                    "Failed to get decimals for token {:?}: {}",
                    token_address, e
                );
                None
            }
        };
        let symbol = contract.symbol().call().await.ok();
        let name = contract.name().call().await.ok();

        (symbol, name, decimals)
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

        // 从链上读取 token 信息
        info!("🔍 链 {}: 读取 token 信息...", self.chain_id);
        let (token0_symbol, token0_name, token0_decimals) =
            self.get_token_info(event.token_0).await;
        let (token1_symbol, token1_name, token1_decimals) =
            self.get_token_info(event.token_1).await;

        let pair = TradingPair {
            id: Uuid::new_v4(),
            chain_id: self.chain_id as i32,
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

        self.database.insert_trading_pair(&pair).await?;

        // 通知前端新交易对
        let _ = self.event_sender.send(serde_json::to_string(&pair)?);

        info!(
            "🎉 链 {}: 新交易对创建 - {} (区块: {})",
            self.chain_id, pair.address, pair.block_number
        );
        info!(
            "   Token0: {} ({}) | Token1: {} ({})",
            pair.token0,
            pair.token0_symbol.as_deref().unwrap_or("Unknown"),
            pair.token1,
            pair.token1_symbol.as_deref().unwrap_or("Unknown")
        );

        // 处理同一块中的 Mint 事件
        if let Err(e) = self
            .handle_mint_events_for_pair(event.pair, block_number.as_u64(), timestamp)
            .await
        {
            warn!(
                "⚠️ 链 {}: 处理交易对 {} 的 Mint 事件失败: {}",
                self.chain_id, pair.address, e
            );
        }

        Ok(())
    }

    // 处理指定交易对在指定区块的 Mint 事件
    async fn handle_mint_events_for_pair(
        &self,
        pair_address: Address,
        block_number: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let filter = Filter::new()
            .address(pair_address)
            .from_block(BlockNumber::Number(block_number.into()))
            .to_block(BlockNumber::Number(block_number.into()))
            .event("Mint(address,uint256,uint256)");

        let logs_opt = self.provider.get_logs(&filter).await.ok();
        let logs = match logs_opt {
            Some(logs) => {
                // 打印 logs
                tracing::debug!(
                    "Found {} Mint event logs for pair 0x{:x} in block {}:",
                    logs.len(),
                    pair_address,
                    block_number
                );
                for (index, log) in logs.iter().enumerate() {
                    tracing::debug!(
                        "Log {}: {{ address: 0x{:x}, topics: {:?}, data: 0x{} }}",
                        index,
                        log.address,
                        log.topics,
                        hex::encode(&log.data)
                    );
                }
                logs
            }
            None => {
                debug!(
                    "链 {}: 交易对 {} 在区块 {} 中没有 Mint 事件",
                    self.chain_id,
                    format!("0x{:x}", pair_address),
                    block_number
                );
                return Ok(());
            }
        };

        if !logs.is_empty() {
            info!(
                "🌱 链 {}: 在区块 {} 中发现交易对 {} 的 {} 个 Mint 事件",
                self.chain_id,
                block_number,
                format!("0x{:x}", pair_address),
                logs.len()
            );
        }

        for log in logs {
            // 验证事件签名
            let mint_event_signature = H256::from_slice(
                &hex::decode("4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f")
                    .unwrap_or_default(),
            );
            if log.topics.get(0) != Some(&mint_event_signature) {
                tracing::debug!(
                    "Skipping non-Mint event for pair 0x{:x} in block {}: topics[0]={:?}",
                    pair_address,
                    block_number,
                    log.topics.get(0)
                );
                continue;
            }

            if let Err(e) = self.handle_mint_event(log, timestamp).await {
                tracing::error!("❌ 链 {}: 处理 Mint 事件失败: {}", self.chain_id, e);
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
            chain_id: self.chain_id as i32,
            pair_address: format!("0x{:x}", log.address),
            sender: format!("0x{:x}", event.sender),
            amount0: Decimal::from(event.amount_0.as_u128()),
            amount1: Decimal::from(event.amount_1.as_u128()),
            block_number: log.block_number.unwrap().as_u64() as i64,
            transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
            log_index: log.log_index.unwrap().as_u32() as i32,
            timestamp,
        };

        self.database.insert_mint_event(&mint_event).await?;
        let _ = self.event_sender.send(serde_json::to_string(&mint_event)?);

        info!(
            "🌱 链 {}: Mint事件 - 交易对: {} (区块: {})",
            self.chain_id, mint_event.pair_address, mint_event.block_number
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

        // Swap事件签名: keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
        let swap_signature = [
            0xd7, 0x8a, 0xd9, 0x5f, 0xa4, 0x6c, 0x99, 0x4b, 0x6e, 0x6f, 0x0d, 0x4a, 0xaa, 0x7c,
            0xe5, 0xbd, 0x1e, 0xdd, 0x3e, 0x86, 0xef, 0x3e, 0x7e, 0x93, 0xb2, 0xa0, 0x8c, 0x5d,
            0x0e, 0x57, 0x9b, 0x9b,
        ];

        // Mint事件签名: keccak256("Mint(address,uint256,uint256)")
        let mint_signature = [
            0x4c, 0x20, 0x9b, 0x5f, 0xc8, 0xad, 0x50, 0x15, 0x8f, 0x35, 0x15, 0x5b, 0x2f, 0xd2,
            0x6b, 0xb6, 0x42, 0x4a, 0x6f, 0xe0, 0x5e, 0x6a, 0x7e, 0x4b, 0x04, 0x2f, 0xeb, 0x5f,
            0x0e, 0x64, 0xec, 0x39,
        ];

        // Burn事件签名: keccak256("Burn(address,uint256,uint256,address)")
        let burn_signature = [
            0xdc, 0xcd, 0x41, 0x2f, 0x0b, 0x12, 0x36, 0xf1, 0x9d, 0x88, 0xf8, 0xf6, 0x10, 0x8f,
            0xda, 0x47, 0xb3, 0x0c, 0x1d, 0x31, 0x11, 0x4b, 0x5a, 0x6c, 0x92, 0x13, 0x49, 0x16,
            0x72, 0xfb, 0x0a, 0x29,
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

            debug!(
                "💱 链 {}: Swap事件 - 交易对: {} (区块: {})",
                self.chain_id, swap_event.pair_address, swap_event.block_number
            );
        } else if event_signature.as_bytes() == mint_signature {
            self.handle_mint_event(log, timestamp).await?;
        } else if event_signature.as_bytes() == burn_signature {
            let sender = Address::from_slice(&log.topics[1][12..]);
            let to = Address::from_slice(&log.topics[2][12..]);

            let data = &log.data;
            let amount0 = U256::from_big_endian(&data[0..32]);
            let amount1 = U256::from_big_endian(&data[32..64]);

            let burn_event = BurnEvent {
                id: Uuid::new_v4(),
                chain_id: self.chain_id as i32,
                pair_address: format!("0x{:x}", pair_address),
                sender: format!("0x{:x}", sender),
                amount0: Decimal::from(amount0.as_u128()),
                amount1: Decimal::from(amount1.as_u128()),
                to_address: format!("0x{:x}", to),
                block_number: log.block_number.unwrap().as_u64() as i64,
                transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
                log_index: log.log_index.unwrap().as_u32() as i32,
                timestamp,
            };

            self.database.insert_burn_event(&burn_event).await?;
            let _ = self.event_sender.send(serde_json::to_string(&burn_event)?);

            debug!(
                "🔥 链 {}: Burn事件 - 交易对: {} (区块: {})",
                self.chain_id, burn_event.pair_address, burn_event.block_number
            );
        }

        Ok(())
    }
}
