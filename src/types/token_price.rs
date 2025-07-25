// TokenPrice, CreateTokenPrice, TokenPriceHistory, PricePoint
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TokenPrice {
    pub id: Uuid,
    pub chain_id: i32,
    pub token_address: String,
    pub token_symbol: String,
    pub price_usd: Decimal,
    pub source: String,
    pub timestamp: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTokenPrice {
    pub chain_id: i32,
    pub token_address: String,
    pub token_symbol: String,
    pub price_usd: Decimal,
    pub source: String,
    pub timestamp: Option<DateTime<Utc>>,
}

// 新增：价格历史查询结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPriceHistory {
    pub token_symbol: String,
    pub prices: Vec<PricePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub price_usd: Decimal,
    pub source: String,
}