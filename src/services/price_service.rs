use crate::database::operations::PriceOperations;
use crate::types::{CreateTokenPrice};
use anyhow::{Result, anyhow};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Duration;
use tokio::time;
use tracing::{info, error, warn};
use chrono::{DateTime, Utc};

#[derive(Debug, Deserialize)]
struct BidacoinResponse {
    code: i32,
    msg: String,
    data: String,
}

#[derive(Debug, Clone)]
pub struct TokenPriceConfig {
    pub symbol: String,
    pub address: String,
    pub chain_id: i32,
    pub api_url: String,
    pub update_interval: Duration,
}

pub struct PriceService {
    pool: PgPool,
    client: reqwest::Client,
    tokens: Vec<TokenPriceConfig>,
}

impl PriceService {
    pub fn new(pool: PgPool) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        // 配置需要更新价格的代币
        let tokens = vec![
            TokenPriceConfig {
                symbol: "NOS".to_string(),
                address: "0x0000000000000000000000000000000000000000".to_string(), // 需要实际的NOS合约地址
                chain_id: 1, // 以太坊主网
                api_url: "https://api.bidacoin.co/api/v0/markets/publicapi/ticker?market=NOSUSDT".to_string(),
                update_interval: Duration::from_secs(60), // 每分钟更新一次
            },
        ];

        Self {
            pool,
            client,
            tokens,
        }
    }

    /// 启动价格更新服务
    pub async fn start(&self) -> Result<()> {
        info!("🚀 启动价格更新服务...");

        // 为每个代币启动独立的更新任务
        let mut handles = Vec::new();

        for token_config in &self.tokens {
            let pool = self.pool.clone();
            let client = self.client.clone();
            let config = token_config.clone();

            let handle = tokio::spawn(async move {
                Self::update_token_price_loop(pool, client, config).await;
            });

            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            if let Err(e) = handle.await {
                error!("价格更新任务错误: {}", e);
            }
        }

        Ok(())
    }

    /// 单个代币的价格更新循环
    async fn update_token_price_loop(
        pool: PgPool,
        client: reqwest::Client,
        config: TokenPriceConfig,
    ) {
        let mut interval = time::interval(config.update_interval);
        
        info!("开始更新 {} 价格，间隔: {:?}", config.symbol, config.update_interval);

        loop {
            interval.tick().await;

            match Self::fetch_and_update_price(&pool, &client, &config).await {
                Ok(price) => {
                    info!("✅ {} 价格更新成功: ${}", config.symbol, price);
                }
                Err(e) => {
                    error!("❌ {} 价格更新失败: {}", config.symbol, e);
                }
            }
        }
    }

    /// 获取并更新单个代币价格
    async fn fetch_and_update_price(
        pool: &PgPool,
        client: &reqwest::Client,
        config: &TokenPriceConfig,
    ) -> Result<Decimal> {
        // 获取价格数据
        let price = Self::fetch_price_from_api(client, &config.api_url, &config.symbol).await?;

        // 插入数据库
        let price_data = CreateTokenPrice {
            chain_id: config.chain_id,
            token_address: config.address.clone(),
            token_symbol: config.symbol.clone(),
            price_usd: price.clone(),
            source: "bidacoin".to_string(),
            timestamp: Some(Utc::now()),
        };

        PriceOperations::insert_token_price(pool, &price_data).await?;

        Ok(price)
    }

    /// 从API获取价格
    async fn fetch_price_from_api(
        client: &reqwest::Client,
        api_url: &str,
        symbol: &str,
    ) -> Result<Decimal> {
        let response = client
            .get(api_url)
            .header("User-Agent", "UniswapMonitor/1.0")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("API请求失败: {}", response.status()));
        }

        let api_response: BidacoinResponse = response.json().await?;

        if api_response.code != 1 {
            return Err(anyhow!("API返回错误: {}", api_response.msg));
        }

        let price = Decimal::from_str(&api_response.data)
            .map_err(|e| anyhow!("价格解析失败: {}", e))?;

        if price <= Decimal::from(0) {
            return Err(anyhow!("无效的价格数据: {}", price));
        }

        Ok(price)
    }

    /// 手动更新所有代币价格
    pub async fn update_all_prices(&self) -> Result<()> {
        info!("🔄 手动更新所有代币价格...");

        for config in &self.tokens {
            match Self::fetch_and_update_price(&self.pool, &self.client, config).await {
                Ok(price) => {
                    info!("✅ {} 价格更新成功: ${}", config.symbol, price);
                }
                Err(e) => {
                    error!("❌ {} 价格更新失败: {}", config.symbol, e);
                }
            }
        }

        Ok(())
    }

    /// 获取代币最新价格
    pub async fn get_latest_price(&self, symbol: &str) -> Result<Option<Decimal>> {
        if let Some(price_record) = PriceOperations::get_latest_price_by_symbol(&self.pool, symbol).await? {
            Ok(Some(price_record.price_usd))
        } else {
            Ok(None)
        }
    }

    /// 清理旧的价格数据（保留最近30天）
    pub async fn cleanup_old_data(&self) -> Result<u64> {
        let cutoff_time = Utc::now() - chrono::Duration::days(30);
        let deleted_count = PriceOperations::cleanup_old_prices(&self.pool, cutoff_time).await?;
        
        if deleted_count > 0 {
            info!("🧹 清理了 {} 条旧价格记录", deleted_count);
        }

        Ok(deleted_count)
    }
}
