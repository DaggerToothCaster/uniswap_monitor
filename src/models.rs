use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
/// 表示去中心化交易所（如 Uniswap）上的一个交易对。
///
/// # 字段说明
/// - `id`: 交易对的唯一标识符。
/// - `address`: 交易对合约的地址。
/// - `token0`: 交易对中第一个代币的地址。
/// - `token1`: 交易对中第二个代币的地址。
/// - `token0_symbol`: 第一个代币的可选符号。
/// - `token1_symbol`: 第二个代币的可选符号。
/// - `token0_decimals`: 第一个代币的可选小数位数。
/// - `token1_decimals`: 第二个代币的可选小数位数。
/// - `created_at`: 交易对创建时的 UTC 时间戳。
/// - `block_number`: 交易对创建时的区块高度。
/// - `transaction_hash`: 创建事件的交易哈希。
pub struct TradingPair {
    pub id: Uuid,
    pub address: String,
    pub token0: String,
    pub token1: String,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub block_number: i64,
    pub transaction_hash: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
/// 表示一次 Uniswap 交换事件的数据结构。
///
/// # 字段
/// - `id`：事件的唯一标识符（UUID）。
/// - `pair_address`：交易对的合约地址。
/// - `sender`：发起交换的账户地址。
/// - `amount0_in`：输入的第一个代币数量。
/// - `amount1_in`：输入的第二个代币数量。
/// - `amount0_out`：输出的第一个代币数量。
/// - `amount1_out`：输出的第二个代币数量。
/// - `to_address`：接收输出代币的账户地址。
/// - `block_number`：事件发生时的区块高度。
/// - `transaction_hash`：包含该事件的交易哈希。
/// - `log_index`：事件在区块日志中的索引。
/// - `timestamp`：事件发生的时间戳（UTC）。
pub struct SwapEvent {
    pub id: Uuid,
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
/// 表示 Uniswap 上的 Mint 事件。
///
/// # 字段
/// - `id`：事件的唯一标识符（UUID）。
/// - `pair_address`：交易对的合约地址。
/// - `sender`：发起 Mint 操作的地址。
/// - `amount0`：本次 Mint 操作中 token0 的数量。
/// - `amount1`：本次 Mint 操作中 token1 的数量。
/// - `block_number`：事件发生时的区块高度。
/// - `transaction_hash`：包含该事件的交易哈希。
/// - `log_index`：事件在区块日志中的索引。
/// - `timestamp`：事件发生的 UTC 时间戳。
pub struct MintEvent {
    pub id: Uuid,
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
/// 表示一次 Uniswap 上的 Burn 事件。
///
/// # 字段
/// - `id`：事件的唯一标识符（UUID）。
/// - `pair_address`：交易对合约地址。
/// - `sender`：发起 Burn 操作的地址。
/// - `amount0`：销毁的第一个代币数量。
/// - `amount1`：销毁的第二个代币数量。
/// - `to_address`：接收剩余资产的地址。
/// - `block_number`：事件发生时的区块高度。
/// - `transaction_hash`：事件所属交易的哈希值。
/// - `log_index`：事件在区块日志中的索引。
/// - `timestamp`：事件发生的时间戳（UTC）。
pub struct BurnEvent {
    pub id: Uuid,
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
/// 表示K线（蜡烛图）数据结构。
///
/// # 字段
/// - `timestamp`：K线的时间戳，使用UTC时间。
/// - `open`：该时间段的开盘价。
/// - `high`：该时间段的最高价。
/// - `low`：该时间段的最低价。
/// - `close`：该时间段的收盘价。
/// - `volume`：该时间段的成交量。
pub struct KlineData {
    pub timestamp: DateTime<Utc>,
    pub open: BigDecimal,
    pub high: BigDecimal,
    pub low: BigDecimal,
    pub close: BigDecimal,
    pub volume: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// 表示一个交易对的统计信息。
///
/// # 字段
/// - `pair_address`：交易对的合约地址。
/// - `token0_symbol`：第一个代币的符号。
/// - `token1_symbol`：第二个代币的符号。
/// - `price`：当前价格。
/// - `volume_24h`：24小时内的交易量。
/// - `liquidity`：当前流动性。
/// - `price_change_24h`：24小时内的价格变动百分比。
pub struct PairStats {
    pub pair_address: String,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub price: BigDecimal,
    pub volume_24h: BigDecimal,
    pub liquidity: BigDecimal,
    pub price_change_24h: BigDecimal,
}
