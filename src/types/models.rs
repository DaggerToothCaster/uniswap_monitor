use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TradingPair {
    pub id: Uuid,
    pub chain_id: i32,
    pub address: String,
    pub token0: String,
    pub token1: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
    pub token0_name: Option<String>,
    pub token1_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub block_number: i64,
    pub transaction_hash: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct LastProcessedBlock {
    pub id: Uuid,
    pub chain_id: i32,
    pub last_block_number: i64,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TokenMetadata {
    pub id: Uuid,
    pub chain_id: i32,
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: i32,
    pub description: Option<String>,
    pub website_url: Option<String>,
    pub logo_url: Option<String>,
    pub twitter_url: Option<String>,
    pub telegram_url: Option<String>,
    pub discord_url: Option<String>,
    pub github_url: Option<String>,
    pub explorer_url: Option<String>,
    pub coingecko_id: Option<String>,
    pub coinmarketcap_id: Option<String>,
    pub total_supply: Option<Decimal>,
    pub max_supply: Option<Decimal>,
    pub is_verified: bool,
    pub tags: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTokenMetadata {
    pub chain_id: i32,
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: i32,
    pub description: Option<String>,
    pub website_url: Option<String>,
    pub logo_url: Option<String>,
    pub twitter_url: Option<String>,
    pub telegram_url: Option<String>,
    pub discord_url: Option<String>,
    pub github_url: Option<String>,
    pub explorer_url: Option<String>,
    pub coingecko_id: Option<String>,
    pub coinmarketcap_id: Option<String>,
    pub total_supply: Option<Decimal>,
    pub max_supply: Option<Decimal>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTokenMetadata {
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub decimals: Option<i32>,
    pub description: Option<String>,
    pub website_url: Option<String>,
    pub logo_url: Option<String>,
    pub twitter_url: Option<String>,
    pub telegram_url: Option<String>,
    pub discord_url: Option<String>,
    pub github_url: Option<String>,
    pub explorer_url: Option<String>,
    pub coingecko_id: Option<String>,
    pub coinmarketcap_id: Option<String>,
    pub total_supply: Option<Decimal>,
    pub max_supply: Option<Decimal>,
    pub is_verified: Option<bool>,
    pub tags: Option<Vec<String>>,
}
