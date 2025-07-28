use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// 交易对
/// token0地址字母序小于token1
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

/// 带统计信息的交易对数据结构
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct TradingPairWithStats {
    pub id: Uuid,
    pub pair_address: String,
    pub chain_id: i32,
    pub token0: String,
    pub token1: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub price: Decimal,                      // token0/token1 价格
    pub inverted_price: Decimal,             // token1/token0 价格
    pub price_24h_change: Decimal,           // 24小时价格变化百分比
    pub volume_24h_token0: Decimal,          // 24小时token0交易量
    pub volume_24h_token1: Decimal,          // 24小时token1交易量
    pub tx_count_24h: i64,                   // 24小时交易次数
    pub liquidity_token0: Decimal,           // token0流动性
    pub liquidity_token1: Decimal,           // token1流动性
    pub last_updated: Option<DateTime<Utc>>, // 最后更新时间
    pub price_usd: Decimal,                  // token0/token1 价格的usd价值
    pub volume_24h_usd: Decimal,             // 24小时 usd交易价值
    pub liquidity_usd: Decimal,              // 流动性 usd价值
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

    pub total_supply: Option<Decimal>,
    pub max_supply: Option<Decimal>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTokenMetadata {
    pub chain_id: i32,
    pub address: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub decimals: Option<i32>,
    pub description: Option<String>,
    pub website_url: Option<String>,
    pub logo_url: Option<String>,
    pub total_supply: Option<Decimal>,
    pub max_supply: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    pub timestamp: DateTime<Utc>,
    pub price: Decimal,
    pub volume: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: Uuid,
    pub chain_id: i32,
    pub pair_address: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
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
    pub price_usd: Option<Decimal>,
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
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
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
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStats {
    pub wallet_address: String,
    pub chain_id: Option<i32>,
    pub total_transactions: i64,
    pub total_volume_usd: Decimal,
    pub total_fees_paid: Decimal,
    pub profit_loss: Decimal,
    pub win_rate: Decimal,
    pub avg_trade_size: Decimal,
    pub first_transaction: DateTime<Utc>,
    pub last_transaction: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPortfolioItem {
    pub token_address: String,
    pub token_symbol: String,
    pub token_name: String,
    pub balance: Decimal,
    pub value_usd: Decimal,
    pub avg_buy_price: Decimal,
    pub current_price: Decimal,
    pub profit_loss: Decimal,
    pub profit_loss_percentage: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPnLRecord {
    pub date: DateTime<Utc>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub volume: Decimal,
    pub fees_paid: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: String,
    pub database_status: String,
    pub event_listeners_status: Vec<EventListenerStatus>,
    pub last_block_processed: i64,
    pub blocks_behind: i64,
    pub uptime_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventListenerStatus {
    pub chain_id: i32,
    pub event_type: String,
    pub status: String,
    pub last_processed_block: i64,
    pub blocks_behind: i64,
    pub last_updated: DateTime<Utc>,
}
