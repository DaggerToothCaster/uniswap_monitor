use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

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