use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

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
pub struct KLineData {
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trade_count: i64,
}
