use crate::types::{CreateTokenPrice, TokenPrice, TokenPriceHistory};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;

pub struct PriceOperations;

impl PriceOperations {
    /// 插入代币价格记录
    pub async fn upsert_token_price(pool: &PgPool, price_data: &CreateTokenPrice) -> Result<()> {
        let timestamp = price_data.timestamp.unwrap_or_else(Utc::now);

        sqlx::query(
            r#"
        INSERT INTO token_prices (
            chain_id, token_address, token_symbol, price_usd, source, timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (chain_id, token_address) 
        DO UPDATE SET
            token_symbol = EXCLUDED.token_symbol,
            price_usd = EXCLUDED.price_usd,
            source = EXCLUDED.source,
            timestamp = EXCLUDED.timestamp
        "#,
        )
        .bind(price_data.chain_id)
        .bind(&price_data.token_address)
        .bind(&price_data.token_symbol)
        .bind(&price_data.price_usd)
        .bind(&price_data.source)
        .bind(timestamp)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// 获取代币最新价格,通过代币地址
    pub async fn get_latest_token_price(
        pool: &PgPool,
        chain_id: i32,
        token_address: &str,
    ) -> Result<Option<TokenPrice>> {
        let row = sqlx::query!(
            r#"
            SELECT id, chain_id, token_address, token_symbol, price_usd, source, timestamp, created_at
            FROM token_prices
            WHERE chain_id = $1 AND token_address = $2
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
            chain_id,
            token_address
        )
        .fetch_optional(pool)
        .await?;

        if let Some(row) = row {
            Ok(Some(TokenPrice {
                id: row.id,
                chain_id: row.chain_id,
                token_address: row.token_address,
                token_symbol: row.token_symbol,
                price_usd: row.price_usd,
                source: row.source,
                timestamp: row.timestamp,
                created_at: row.created_at,
            }))
        } else {
            Ok(None)
        }
    }

    /// 根据代币符号获取最新价格，通过代币符号
    pub async fn get_latest_price_by_symbol(
        pool: &PgPool,
        token_symbol: &str,
    ) -> Result<Option<TokenPrice>> {
        let row = sqlx::query!(
            r#"
            SELECT id, chain_id, token_address, token_symbol, price_usd, source, timestamp, created_at
            FROM token_prices
            WHERE token_symbol = $1
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
            token_symbol
        )
        .fetch_optional(pool)
        .await?;

        if let Some(row) = row {
            Ok(Some(TokenPrice {
                id: row.id,
                chain_id: row.chain_id,
                token_address: row.token_address,
                token_symbol: row.token_symbol,
                price_usd: row.price_usd,
                source: row.source,
                timestamp: row.timestamp,
                created_at: row.created_at,
            }))
        } else {
            Ok(None)
        }
    }
}
