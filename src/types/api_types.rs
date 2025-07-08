use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::TokenMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub transaction_hash: String,
    pub sender: String,
    pub to_address: String,
    pub amount0_in: Decimal,
    pub amount1_in: Decimal,
    pub amount0_out: Decimal,
    pub amount1_out: Decimal,
    pub price: Decimal,
    pub trade_type: String,
    pub volume_usd: Option<Decimal>,
    pub block_number: i64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityRecord {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub transaction_hash: String,
    pub sender: String,
    pub to_address: Option<String>,
    pub amount0: Decimal,
    pub amount1: Decimal,
    pub liquidity_type: String,
    pub value_usd: Option<Decimal>,
    pub block_number: i64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletTransaction {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub transaction_hash: String,
    pub wallet_address: String,
    pub transaction_type: String,
    pub amount0: Decimal,
    pub amount1: Decimal,
    pub price: Option<Decimal>,
    pub value_usd: Option<Decimal>,
    pub block_number: i64,
    pub timestamp: DateTime<Utc>,
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
    pub token0_logo_url: Option<String>,
    pub token1_logo_url: Option<String>,
    pub token0_website_url: Option<String>,
    pub token1_website_url: Option<String>,
    pub token0_explorer_url: Option<String>,
    pub token1_explorer_url: Option<String>,
    pub token0_description: Option<String>,
    pub token1_description: Option<String>,
    pub token0_tags: Option<Vec<String>>,
    pub token1_tags: Option<Vec<String>>,
    pub price_usd: Decimal,
    pub price_change_1h: Decimal,
    pub price_change_24h: Decimal,
    pub volume_1h: Decimal,
    pub volume_24h: Decimal,
    pub fdv: Option<Decimal>,
    pub market_cap: Option<Decimal>,
    pub liquidity: Decimal,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDetail {
    pub metadata: TokenMetadata,
    pub price_info: Option<TokenPriceInfo>,
    pub trading_pairs: Vec<TradingPairInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPriceInfo {
    pub current_price: Decimal,
    pub price_change_1h: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_7d: Decimal,
    pub volume_24h: Decimal,
    pub market_cap: Option<Decimal>,
    pub fdv: Option<Decimal>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPairInfo {
    pub pair_address: String,
    pub other_token_symbol: String,
    pub other_token_name: String,
    pub price: Decimal,
    pub volume_24h: Decimal,
    pub liquidity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairStats {
    pub pair_address: String,
    pub chain_id: i32,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub price: Decimal,
    pub volume_24h: Decimal,
    pub liquidity: Decimal,
    pub price_change_24h: Decimal,
    pub tx_count_24h: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStats {
    pub chain_id: i32,
    pub chain_name: String,
    pub total_pairs: i64,
    pub total_volume_24h: Decimal,
    pub total_liquidity: Decimal,
    pub active_pairs_24h: i64,
}
