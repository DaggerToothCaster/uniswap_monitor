use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use bigdecimal::BigDecimal;

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
pub struct SwapEvent {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub sender: String,
    pub amount0_in: BigDecimal,
    pub amount1_in: BigDecimal,
    pub amount0_out: BigDecimal,
    pub amount1_out: BigDecimal,
    pub to_address: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub log_index: i32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MintEvent {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub sender: String,
    pub amount0: BigDecimal,
    pub amount1: BigDecimal,
    pub block_number: i64,
    pub transaction_hash: String,
    pub log_index: i32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct BurnEvent {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub sender: String,
    pub amount0: BigDecimal,
    pub amount1: BigDecimal,
    pub to_address: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub log_index: i32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub timestamp: DateTime<Utc>,
    pub open: BigDecimal,
    pub high: BigDecimal,
    pub low: BigDecimal,
    pub close: BigDecimal,
    pub volume: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenListItem {
    pub rank: i32,
    pub chain_id: i32,
    pub chain_name: String,
    pub pair_address: String,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub token0_name: String,
    pub token1_name: String,
    pub price_usd: BigDecimal,
    pub price_change_1h: BigDecimal,
    pub price_change_24h: BigDecimal,
    pub volume_1h: BigDecimal,
    pub volume_24h: BigDecimal,
    pub fdv: Option<BigDecimal>, // Fully Diluted Valuation
    pub market_cap: Option<BigDecimal>,
    pub liquidity: BigDecimal,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairStats {
    pub pair_address: String,
    pub chain_id: i32,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub price: BigDecimal,
    pub volume_24h: BigDecimal,
    pub liquidity: BigDecimal,
    pub price_change_24h: BigDecimal,
    pub tx_count_24h: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStats {
    pub chain_id: i32,
    pub chain_name: String,
    pub total_pairs: i64,
    pub total_volume_24h: BigDecimal,
    pub total_liquidity: BigDecimal,
    pub active_pairs_24h: i64,
}
