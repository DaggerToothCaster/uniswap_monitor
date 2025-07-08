use crate::database::Database;
use crate::models::*;
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

// 修正 UniswapV2Pair ABI - 使用正确的参数名称
abigen!(
    UniswapV2Pair,
    r#"[
        event Mint(address indexed sender, uint256 amount0, uint256 amount1)
        event Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)
        event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
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
    // 新增：区块批次大小配置
    factory_block_batch_size: u64,
    pair_block_batch_size: u64,
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
        factory_block_batch_size: u64,
        pair_block_batch_size: u64,
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
            factory_block_batch_size,
            pair_block_batch_size,
        }
    }

    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("🚀 启动链 {} 的事件监控服务...", self.chain_id);
        info!("📊 配置信息 - 工厂批次大小: {}, 交易对批次大小: {}", 
              self.factory_block_batch_size, self.pair_block_batch_size);

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

        // 开始轮询循环
        loop {
            // 每次循环都重新加载交易对，以便获取新创建的交易对
            let pairs = self
                .database
                .get_all_pairs(Some(self.chain_id as i32))
                .await?;
            let pair_addresses: Vec<Address> = pairs
                .iter()
                .filter_map(|p| {
                    match p.address.parse::<Address>() {
                        Ok(addr) => Some(addr),
                        Err(e) => {
                            warn!("链 {}: 无法解析交易对地址 '{}': {}", self.chain_id, p.address, e);
                            None
                        }
                    }
                })
                .collect();

            info!(
                "📊 链 {}: 当前监控 {} 个交易对",
                self.chain_id,
                pair_addresses.len()
            );

            // 打印前几个交易对地址用于调试
            if !pair_addresses.is_empty() {
                info!("🔍 链 {}: 监控的交易对示例:", self.chain_id);
                for (i, addr) in pair_addresses.iter().take(5).enumerate() {
                    info!("  {}. 0x{:x}", i + 1, addr);
                }
                if pair_addresses.len() > 5 {
                    info!("  ... 还有 {} 个交易对", pair_addresses.len() - 5);
                }
            }

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

        info!(
            "📊 链 {}: 手动处理时监控 {} 个交易对",
            self.chain_id,
            pair_addresses.len()
        );

        // 分批处理，使用配置的批次大小
        let mut current_from = from_block;

        while current_from <= to_block {
            let current_to = std::cmp::min(current_from + self.factory_block_batch_size - 1, to_block);

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
            if let Err(e) = self.poll_pair_events(&pair_addresses, current_from, current_to).await
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
        // 使用配置的批次大小限制每次处理的区块数量
        let to_block = std::cmp::min(from_block + self.pair_block_batch_size - 1, latest_block);

        info!(
            "🔍 链 {}: 处理区块 {} 到 {} (共 {} 个区块)",
            self.chain_id,
            from_block,
            to_block,
            to_block - from_block + 1
        );

        let mut has_error = false;

        // 轮询工厂合约的新交易对事件 - 失败不阻止后续处理
        if let Err(e) = self.poll_factory_events(from_block, to_block).await {
            error!("❌ 链 {}: 处理工厂事件失败: {}", self.chain_id, e);
            has_error = true;
            // 不要 return，继续处理交易对事件
        }

        // 轮询现有交易对的事件 - 失败不阻止区块更新
        if let Err(e) = self.poll_pair_events(existing_pairs, from_block, to_block).await {
            error!("❌ 链 {}: 处理交易对事件失败: {}", self.chain_id, e);
            has_error = true;
            // 不要 return，继续更新区块
        }

        // 即使有错误，也要更新最后处理的区块，避免重复处理
        self.last_processed_block = to_block;
        if let Err(e) = self
            .database
            .update_last_processed_block(self.chain_id as i32, to_block)
            .await
        {
            error!("❌ 链 {}: 更新最后处理区块失败: {}", self.chain_id, e);
            return Err(e); // 这个错误比较严重，需要返回
        }

        if has_error {
            warn!("⚠️ 链 {}: 区块 {} 处理完成，但有部分错误", self.chain_id, to_block);
        } else {
            debug!("✅ 链 {}: 成功处理到区块 {}", self.chain_id, to_block);
        }

        // 如果还有更多区块需要处理，显示进度
        if to_block < latest_block {
            let remaining = latest_block - to_block;
            info!("📈 链 {}: 处理进度 - 剩余 {} 个区块", self.chain_id, remaining);
        }

        Ok(())
    }

    async fn poll_factory_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        let filter = Filter::new()
            .address(self.factory_address)
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .event("PairCreated(address,address,address,uint256)");

        info!(
            "🏭 链 {}: 查询工厂事件 - 地址: 0x{:x}, 区块: {}-{} (批次大小: {})",
            self.chain_id, self.factory_address, from_block, to_block, 
            to_block - from_block + 1
        );

        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => {
                // 检查 logs 是否为空或 null
                if logs.is_empty() {
                    debug!(
                        "🏭 链 {}: 区块 {}-{} 中没有发现工厂事件",
                        self.chain_id, from_block, to_block
                    );
                    return Ok(());
                }
                logs
            },
            Err(e) => {
                error!(
                    "❌ 链 {}: 获取工厂事件失败: {}",
                    self.chain_id, e
                );
                // 不��直接返回错误，记录错误但继续处理
                warn!("⚠️ 链 {}: 跳过工厂事件处理，继续处理交易对事件", self.chain_id);
                return Err(e.into());
            }
        };

        info!(
            "🏭 链 {}: 发现 {} 个新交易对创建事件",
            self.chain_id,
            logs.len()
        );

        let mut processed = 0;
        let mut failed = 0;

        for (index, log) in logs.iter().enumerate() {
            if let Err(e) = self.handle_pair_created_event(log.clone()).await {
                error!(
                    "❌ 链 {}: 处理第 {} 个PairCreated事件失败: {}",
                    self.chain_id,
                    index + 1,
                    e
                );
                failed += 1;
                // 继续处理其他事件，不要因为一个事件失败就停止
            } else {
                processed += 1;
            }
        }

        if failed > 0 {
            warn!(
                "⚠️ 链 {}: 工厂事件处理完成 - 成功: {}, 失败: {}",
                self.chain_id, processed, failed
            );
        } else if processed > 0 {
            info!(
                "✅ 链 {}: 工厂事件处理完成 - 成功处理 {} 个事件",
                self.chain_id, processed
            );
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
            info!("📭 链 {}: 没有交易对需要监控", self.chain_id);
            return Ok(());
        }

        info!(
            "💱 链 {}: 开始查询 {} 个交易对的事件 (区块 {}-{}, 批次大小: {})",
            self.chain_id,
            pair_addresses.len(),
            from_block,
            to_block,
            to_block - from_block + 1
        );

        let mut total_events = 0;
        let mut failed_pairs = 0;
        let mut successful_pairs = 0;

        // 改为逐个处理交易对地址，避免批量查询的兼容性问题
        for (index, &pair_address) in pair_addresses.iter().enumerate() {
            info!(
                "🔍 链 {}: 处理第 {} 个交易对: 0x{:x}",
                self.chain_id,
                index + 1,
                pair_address
            );

            // 为单个交易对创建过滤器
            let filter = Filter::new()
                .address(pair_address) // 使用单个地址而不是数组
                .from_block(BlockNumber::Number(from_block.into()))
                .to_block(BlockNumber::Number(to_block.into()));

            info!(
                "🔍 链 {}: 发送日志查询请求 - 交易对: 0x{:x}, 区块: {}-{}",
                self.chain_id,
                pair_address,
                from_block,
                to_block
            );

            let logs = match self.provider.get_logs(&filter).await {
                Ok(logs) => {
                    // 检查 logs 是否为空或 null
                    if logs.is_empty() {
                        debug!(
                            "📭 链 {}: 交易对 0x{:x} 在区块 {}-{} 中没有发现任何事件",
                            self.chain_id,
                            pair_address,
                            from_block,
                            to_block
                        );
                        continue; // 跳过这个交易对，继续处理下一个
                    }

                    info!(
                        "✅ 链 {}: 交易对 0x{:x} 查询成功，获得 {} 个日志",
                        self.chain_id,
                        pair_address,
                        logs.len()
                    );
                    successful_pairs += 1;
                    total_events += logs.len();

                    // 按事件类型统计
                    let mut event_counts: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();

                    for log in &logs {
                        let event_type = self.get_event_type_from_signature(&log.topics[0]);
                        *event_counts.entry(event_type).or_insert(0) += 1;
                    }

                    // 打印事件统计
                    for (event_type, count) in &event_counts {
                        info!(
                            "📊 链 {}: 交易对 0x{:x} 在区块 {}-{} 中获取到 {} 个 {} 事件",
                            self.chain_id, pair_address, from_block, to_block, count, event_type
                        );
                    }

                    logs
                }
                Err(e) => {
                    error!(
                        "❌ 链 {}: 交易对 0x{:x} 查询失败: {}",
                        self.chain_id,
                        pair_address,
                        e
                    );
                    failed_pairs += 1;
                    // 继续处理下一个交易对，不要因为一个交易对失败就停止
                    continue;
                }
            };

            let mut processed_in_pair = 0;
            let mut failed_in_pair = 0;

            for (log_index, log) in logs.iter().enumerate() {
                debug!(
                    "🔍 链 {}: 处理交易对 0x{:x} 的第 {} 个事件 - 事件签名: 0x{}",
                    self.chain_id,
                    pair_address,
                    log_index + 1,
                    hex::encode(log.topics[0].as_bytes())
                );

                if let Err(e) = self.handle_pair_event(log.clone()).await {
                    error!(
                        "❌ 链 {}: 处理交易对 0x{:x} 第 {} 个事件失败: {}",
                        self.chain_id,
                        pair_address,
                        log_index + 1,
                        e
                    );
                    failed_in_pair += 1;
                    // 继续处理其他事件
                } else {
                    processed_in_pair += 1;
                }
            }

            if failed_in_pair > 0 {
                warn!(
                    "⚠️ 链 {}: 交易对 0x{:x} 处理完成 - 成功: {}, 失败: {}",
                    self.chain_id, pair_address, processed_in_pair, failed_in_pair
                );
            } else if processed_in_pair > 0 {
                info!(
                    "✅ 链 {}: 交易对 0x{:x} 处理完成 - 成功处理 {} 个事件",
                    self.chain_id, pair_address, processed_in_pair
                );
            }

            // 添加小延迟，避免请求过于频繁
            if index < pair_addresses.len() - 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // 总结处理结果
        info!(
            "📊 链 {}: 交易对事件处理总结 - 成功交易对: {}, 失败交易对: {}, 总事件数: {}",
            self.chain_id, successful_pairs, failed_pairs, total_events
        );

        // 只有在所有交易对都失败时才返回错误
        if successful_pairs == 0 && failed_pairs > 0 {
            return Err(anyhow::anyhow!("所有交易对事件查询都失败了"));
        }

        Ok(())
    }

    // 辅助函数：根据事件签名获取事件类型名称
    fn get_event_type_from_signature(&self, signature: &H256) -> String {
        // 计算事件签名 - 使用正确的参数名称
        let swap_signature = H256::from(keccak256("Swap(address,uint256,uint256,uint256,uint256,address)"));
        let mint_signature = H256::from(keccak256("Mint(address,uint256,uint256)"));
        let burn_signature = H256::from(keccak256("Burn(address,uint256,uint256,address)"));

        if *signature == swap_signature {
            "Swap".to_string()
        } else if *signature == mint_signature {
            "Mint".to_string()
        } else if *signature == burn_signature {
            "Burn".to_string()
        } else {
            format!("Unknown(0x{})", hex::encode(signature.as_bytes()))
        }
    }

    async fn get_token_info(
        &self,
        token_address: Address,
    ) -> (Option<String>, Option<String>, Option<i32>) {
        let contract = ERC20Fixed::new(token_address, Arc::clone(&self.provider));

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
                warn!(
                    "Failed to get name for token 0x{:x}: {}",
                    token_address, e
                );
                None
            }
        };

        (symbol, name, decimals)
    }

    // 新增：获取交易对的储备量和初始价格
    async fn get_pair_reserves(&self, pair_address: Address) -> Result<(U256, U256, Decimal)> {
        let contract = UniswapV2Pair::new(pair_address, Arc::clone(&self.provider));
        
        let (reserve0, reserve1, _) = contract.get_reserves().call().await?;
        
        let reserve0_u256 = U256::from(reserve0);
        let reserve1_u256 = U256::from(reserve1);
        
        // 计算初始价格 (token1/token0)
        let price = if reserve0_u256 > U256::zero() {
            let price_ratio = reserve1_u256.as_u128() as f64 / reserve0_u256.as_u128() as f64;
            Decimal::try_from(price_ratio).unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        
        info!(
            "💰 链 {}: 交易对 0x{:x} 储备量 - Reserve0: {}, Reserve1: {}, 价格: {}",
            self.chain_id, pair_address, reserve0_u256, reserve1_u256, price
        );
        
        Ok((reserve0_u256, reserve1_u256, price))
    }

    // 新增：创建初始 K 线数据
    async fn create_initial_kline(&self, pair_address: Address, timestamp: DateTime<Utc>, initial_price: Decimal) -> Result<()> {
        if initial_price > Decimal::ZERO {
            let kline = KlineData {
                timestamp,
                open: initial_price,
                high: initial_price,
                low: initial_price,
                close: initial_price,
                volume: Decimal::ZERO, // 初始创建时没有交易量
            };

            // 这里需要在数据库中添加保存 K 线数据的方法
            info!(
                "📈 链 {}: 为交易对 0x{:x} 创建初始K线 - 价格: {}, 时间: {}",
                self.chain_id, pair_address, initial_price, timestamp
            );
            
            // TODO: 调用数据库方法保存 K 线数据
            // self.database.insert_kline_data(self.chain_id as i32, &format!("0x{:x}", pair_address), &kline).await?;
        }
        
        Ok(())
    }

    // 新增：手动解析 Swap 事件数据
    fn parse_swap_event_manually(&self, log: &Log) -> Result<(Address, U256, U256, U256, U256, Address)> {
        // Swap 事件的结构：
        // event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
        
        if log.topics.len() < 3 {
            return Err(anyhow::anyhow!("Swap 事件 topics 数量不足"));
        }

        // 解析 indexed 参数
        let sender = Address::from(log.topics[1]);
        let to = Address::from(log.topics[2]);

        // 解析 data 中的非 indexed 参数
        if log.data.0.len() < 128 {
            return Err(anyhow::anyhow!("Swap 事件 data 长度不足"));
        }

        // 每个 uint256 占用 32 字节
        let amount0_in = U256::from_big_endian(&log.data.0[0..32]);
        let amount1_in = U256::from_big_endian(&log.data.0[32..64]);
        let amount0_out = U256::from_big_endian(&log.data.0[64..96]);
        let amount1_out = U256::from_big_endian(&log.data.0[96..128]);

        info!(
            "🔍 链 {}: 手动解析 Swap 事件 - sender: 0x{:x}, to: 0x{:x}",
            self.chain_id, sender, to
        );
        info!(
            "   amount0In: {}, amount1In: {}, amount0Out: {}, amount1Out: {}",
            amount0_in, amount1_in, amount0_out, amount1_out
        );

        Ok((sender, amount0_in, amount1_in, amount0_out, amount1_out, to))
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
        let (token0_symbol, token0_name, token0_decimals) = self.get_token_info(event.token_0).await;
        let (token1_symbol, token1_name, token1_decimals) = self.get_token_info(event.token_1).await;

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

        // 获取交易对的初始储备量和价格
        let pair_addr: Address = event.pair;
        if let Ok((reserve0, reserve1, initial_price)) = self.get_pair_reserves(pair_addr).await {
            if reserve0 > U256::zero() && reserve1 > U256::zero() {
                // 创建初始 K 线数据
                if let Err(e) = self.create_initial_kline(pair_addr, timestamp, initial_price).await {
                    warn!(
                        "⚠️ 链 {}: 创建交易对 {} 的初始K线失败: {}",
                        self.chain_id, pair.address, e
                    );
                }
            }
        }

        // 立即检查这个新创建的交易对是否有事件
        info!(
            "🔍 链 {}: 立即检查新交易对 0x{:x} 在区块 {} 的事件",
            self.chain_id, pair_addr, block_number
        );

        // 检查同一区块的事件
        if let Err(e) = self
            .poll_pair_events(&[pair_addr], block_number.as_u64(), block_number.as_u64())
            .await
        {
            warn!(
                "⚠️ 链 {}: 检查新交易对 {} 的事件失败: {}",
                self.chain_id, pair.address, e
            );
        }

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

        // 使用 keccak256 计算正确的事件签名
        let swap_signature = H256::from(keccak256("Swap(address,uint256,uint256,uint256,uint256,address)"));
        let mint_signature = H256::from(keccak256("Mint(address,uint256,uint256)"));
        let burn_signature = H256::from(keccak256("Burn(address,uint256,uint256,address)"));

        info!(
            "🔍 链 {}: 处理事件 - 交易对: 0x{:x}, 事件签名: 0x{}, 区块: {}",
            self.chain_id,
            pair_address,
            hex::encode(event_signature.as_bytes()),
            block_number
        );

        // 打印预期的事件签名用于对比
        debug!(
            "🔍 链 {}: 预期签名 - Swap: 0x{}, Mint: 0x{}, Burn: 0x{}",
            self.chain_id,
            hex::encode(swap_signature.as_bytes()),
            hex::encode(mint_signature.as_bytes()),
            hex::encode(burn_signature.as_bytes())
        );

        if *event_signature == swap_signature {
            info!("✅ 链 {}: 识别为 Swap 事件", self.chain_id);

            // 尝试使用 ABI 解码
            match SwapFilter::decode_log(&RawLog {
                topics: log.topics.clone(),
                data: log.data.0.to_vec(),
            }) {
                Ok(event) => {
                    info!(
                        "✅ 链 {}: ABI 解码成功 - amount0In: {}, amount1In: {}, amount0Out: {}, amount1Out: {}",
                        self.chain_id, event.amount_0_in, event.amount_1_in, event.amount_0_out, event.amount_1_out
                    );

                    let swap_event = SwapEvent {
                        id: Uuid::new_v4(),
                        chain_id: self.chain_id as i32,
                        pair_address: format!("0x{:x}", pair_address),
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

                    self.database.insert_swap_event(&swap_event).await?;
                    let _ = self.event_sender.send(serde_json::to_string(&swap_event)?);

                    info!(
                        "💱 链 {}: Swap事件已保存 - 交易对: {} (区块: {})",
                        self.chain_id, swap_event.pair_address, swap_event.block_number
                    );
                }
                Err(e) => {
                    warn!(
                        "⚠️ 链 {}: ABI 解码失败，尝试手动解析: {}",
                        self.chain_id, e
                    );

                    // 尝试手动解析
                    match self.parse_swap_event_manually(&log) {
                        Ok((sender, amount0_in, amount1_in, amount0_out, amount1_out, to)) => {
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
                                "💱 链 {}: Swap事件已保存(手动解析) - 交易对: {} (区块: {})",
                                self.chain_id, swap_event.pair_address, swap_event.block_number
                            );
                        }
                        Err(manual_err) => {
                            error!(
                                "❌ 链 {}: 手动解析也失败: {}",
                                self.chain_id, manual_err
                            );
                            
                            // 打印原始数据用于调试
                            error!(
                                "🔍 链 {}: 原始 Swap 事件数据 - topics: {:?}, data: 0x{}",
                                self.chain_id,
                                log.topics.iter().map(|t| format!("0x{}", hex::encode(t.as_bytes()))).collect::<Vec<_>>(),
                                hex::encode(&log.data.0)
                            );
                        }
                    }
                }
            }
        } else if *event_signature == mint_signature {
            info!("✅ 链 {}: 识别为 Mint 事件", self.chain_id);
            self.handle_mint_event(log, timestamp).await?;
        } else if *event_signature == burn_signature {
            info!("✅ 链 {}: 识别为 Burn 事件", self.chain_id);

            let event = BurnFilter::decode_log(&RawLog {
                topics: log.topics.clone(),
                data: log.data.0.to_vec(),
            })?;

            let burn_event = BurnEvent {
                id: Uuid::new_v4(),
                chain_id: self.chain_id as i32,
                pair_address: format!("0x{:x}", pair_address),
                sender: format!("0x{:x}", event.sender),
                amount0: Decimal::from(event.amount_0.as_u128()),
                amount1: Decimal::from(event.amount_1.as_u128()),
                to_address: format!("0x{:x}", event.to),
                block_number: log.block_number.unwrap().as_u64() as i64,
                transaction_hash: format!("0x{:x}", log.transaction_hash.unwrap()),
                log_index: log.log_index.unwrap().as_u32() as i32,
                timestamp,
            };

            self.database.insert_burn_event(&burn_event).await?;
            let _ = self.event_sender.send(serde_json::to_string(&burn_event)?);

            info!(
                "🔥 链 {}: Burn事件已保存 - 交易对: {} (区块: {})",
                self.chain_id, burn_event.pair_address, burn_event.block_number
            );
        } else {
            warn!(
                "❓ 链 {}: 未知事件类型 - 交易对: 0x{:x}, 签名: 0x{}",
                self.chain_id,
                pair_address,
                hex::encode(event_signature.as_bytes())
            );
            
            // 尝试打印原始日志数据以便调试
            debug!(
                "🔍 链 {}: 原始日志数据 - topics: {:?}, data: 0x{}",
                self.chain_id,
                log.topics.iter().map(|t| format!("0x{}", hex::encode(t.as_bytes()))).collect::<Vec<_>>(),
                hex::encode(&log.data.0)
            );
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
            "🌱 链 {}: Mint事件已保存 - 交易对: {} (区块: {})",
            self.chain_id, mint_event.pair_address, mint_event.block_number
        );
        info!(
            "   详情: amount0={}, amount1={}",
            mint_event.amount0, mint_event.amount1
        );

        Ok(())
    }
}
