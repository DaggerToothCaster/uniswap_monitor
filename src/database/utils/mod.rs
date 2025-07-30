//! 数据库工具函数模块
//! 
//! 包含各种数据处理、转换和计算的工具函数

pub mod token_reordering;
pub mod price_calculator;
pub mod amount_converter;
pub mod trade_analyzer;
pub mod usd_estimator;
pub mod data_processor;

// 重新导出常用的工具函数和结构体
pub use token_reordering::{TokenReorderingTool, QuoteTokenConfig};
pub use price_calculator::{PriceCalculator, PriceCalculationResult};
pub use amount_converter::{AmountConverter, TokenAmount};
pub use trade_analyzer::{TradeAnalyzer, TradeType, TradeDirection};
pub use usd_estimator::{UsdEstimator, UsdEstimationResult};
pub use data_processor::{DataProcessor, BatchProcessor};

// 原有的安全获取函数保持不变
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use sqlx::Row;
use sqlx::postgres::PgRow;






pub fn safe_get_string(row: &PgRow, column: &str) -> String {
    row.try_get::<String, _>(column)
        .unwrap_or_else(|_| "".to_string())
}

pub fn safe_get_optional_string(row: &PgRow, column: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(column).ok().flatten()
}

pub fn safe_get_i32(row: &PgRow, column: &str) -> i32 {
    row.try_get::<i32, _>(column).unwrap_or(0)
}

pub fn safe_get_i64(row: &PgRow, column: &str) -> i64 {
    row.try_get::<i64, _>(column).unwrap_or(0)
}

pub fn safe_get_decimal(row: &PgRow, column: &str) -> Decimal {
    row.try_get::<Decimal, _>(column)
        .unwrap_or_else(|_| Decimal::ZERO)
}

pub fn safe_get_optional_decimal(row: &PgRow, column: &str) -> Option<Decimal> {
    row.try_get::<Option<Decimal>, _>(column).ok().flatten()
}

pub fn safe_get_datetime(row: &PgRow, column: &str) -> DateTime<Utc> {
    row.try_get::<DateTime<Utc>, _>(column)
        .unwrap_or_else(|_| Utc::now())
}

pub fn safe_get_bool(row: &PgRow, column: &str) -> bool {
    row.try_get::<bool, _>(column).unwrap_or(false)
}

pub fn safe_get_uuid(row: &PgRow, column: &str) -> Uuid {
    row.try_get::<Uuid, _>(column)
        .unwrap_or_else(|_| Uuid::new_v4())
}

pub fn safe_get_optional_i32(row: &PgRow, column: &str) -> Option<i32> {
    row.try_get::<Option<i32>, _>(column).ok().flatten()
}

pub fn safe_get_optional_datetime(row: &PgRow, column: &str) -> Option<DateTime<Utc>> {
    row.try_get::<Option<DateTime<Utc>>, _>(column).ok().flatten()
}