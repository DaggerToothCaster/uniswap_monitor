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
    pub event_type: String,
    pub last_block_number: i64,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ProcessingStatus {
    pub chain_id: i32,
    pub chain_name: String,
    pub factory_block: Option<i64>,
    pub swap_block: Option<i64>,
    pub min_processed_block: Option<i64>,
    pub max_processed_block: Option<i64>,
    pub factory_updated_at: Option<DateTime<Utc>>,
    pub swap_updated_at: Option<DateTime<Utc>>,
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
    pub tags: Option<Vec<String>>, // JSON array stored as text
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

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SwapEvent {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub sender: String,
    pub amount0_in: Decimal,
    pub amount1_in: Decimal,
    pub amount0_out: Decimal,
    pub amount1_out: Decimal,
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
    pub amount0: Decimal,
    pub amount1: Decimal,
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
    pub amount0: Decimal,
    pub amount1: Decimal,
    pub to_address: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub log_index: i32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trade_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    pub timestamp: DateTime<Utc>,
    pub price: Decimal,
    pub volume: Decimal,
}

// 新增：交易记录详情
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
    pub trade_type: String, // "buy" or "sell"
    pub volume_usd: Option<Decimal>,
    pub block_number: i64,
    pub timestamp: DateTime<Utc>,
}

// 新增：流动性记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityRecord {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub transaction_hash: String,
    pub sender: String,
    pub to_address: Option<String>, // burn事件才有
    pub amount0: Decimal,
    pub amount1: Decimal,
    pub liquidity_type: String, // "add" or "remove"
    pub value_usd: Option<Decimal>,
    pub block_number: i64,
    pub timestamp: DateTime<Utc>,
}

// 新增：钱包交易记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletTransaction {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub transaction_hash: String,
    pub wallet_address: String,
    pub transaction_type: String, // "swap", "mint", "burn"
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
