use crate::types::{TokenPrice, CreateTokenPrice, TokenPriceHistory, PricePoint};
use anyhow::Result;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

pub struct PriceOperations;

impl PriceOperations {
    /// 插入代币价格记录
    pub async fn insert_token_price(
        pool: &PgPool,
        price_data: &CreateTokenPrice,
    ) -> Result<TokenPrice> {
        let timestamp = price_data.timestamp.unwrap_or_else(Utc::now);
        
        let row = sqlx::query!(
            r#"
            INSERT INTO token_prices (
                chain_id, token_address, token_symbol, price_usd, source, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, chain_id, token_address, token_symbol, price_usd, source, timestamp, created_at
            "#,
            price_data.chain_id,
            price_data.token_address,
            price_data.token_symbol,
            price_data.price_usd,
            price_data.source,
            timestamp
        )
        .fetch_one(pool)
        .await?;

        Ok(TokenPrice {
            id: row.id,
            chain_id: row.chain_id,
            token_address: row.token_address,
            token_symbol: row.token_symbol,
            price_usd: row.price_usd,
            source: row.source,
            timestamp: row.timestamp,
            created_at: row.created_at,
        })
    }

    /// 获取代币最新价格
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

    /// 根据代币符号获取最新价格
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

    /// 获取代币价格历史
    pub async fn get_token_price_history(
        pool: &PgPool,
        token_symbol: &str,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> Result<TokenPriceHistory> {
        let mut query = String::from(
            r#"
            SELECT timestamp, price_usd, source
            FROM token_prices
            WHERE token_symbol = $1
            "#
        );

        let mut param_count = 1;
        
        if start_time.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND timestamp >= ${}", param_count));
        }
        
        if end_time.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND timestamp <= ${}", param_count));
        }
        
        query.push_str(" ORDER BY timestamp DESC");
        
        if let Some(limit) = limit {
            param_count += 1;
            query.push_str(&format!(" LIMIT ${}", param_count));
        }

        let mut query_builder = sqlx::query(&query).bind(token_symbol);
        
        if let Some(start) = start_time {
            query_builder = query_builder.bind(start);
        }
        
        if let Some(end) = end_time {
            query_builder = query_builder.bind(end);
        }
        
        if let Some(limit) = limit {
            query_builder = query_builder.bind(limit);
        }

        let rows = query_builder.fetch_all(pool).await?;

        let prices: Vec<PricePoint> = rows
            .into_iter()
            .map(|row| PricePoint {
                timestamp: row.get("timestamp"),
                price_usd: row.get("price_usd"),
                source: row.get("source"),
            })
            .collect();

        Ok(TokenPriceHistory {
            token_symbol: token_symbol.to_string(),
            prices,
        })
    }

    /// 批量插入价格数据
    pub async fn batch_insert_prices(
        pool: &PgPool,
        prices: &[CreateTokenPrice],
    ) -> Result<Vec<Uuid>> {
        let mut tx = pool.begin().await?;
        let mut ids = Vec::new();

        for price in prices {
            let timestamp = price.timestamp.unwrap_or_else(Utc::now);
            
            let row = sqlx::query!(
                r#"
                INSERT INTO token_prices (
                    chain_id, token_address, token_symbol, price_usd, source, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                "#,
                price.chain_id,
                price.token_address,
                price.token_symbol,
                price.price_usd,
                price.source,
                timestamp
            )
            .fetch_one(&mut *tx)
            .await?;

            ids.push(row.id);
        }

        tx.commit().await?;
        Ok(ids)
    }

    /// 清理旧的价格数据
    pub async fn cleanup_old_prices(
        pool: &PgPool,
        before_timestamp: DateTime<Utc>,
    ) -> Result<u64> {
        let result = sqlx::query!(
            "DELETE FROM token_prices WHERE timestamp < $1",
            before_timestamp
        )
        .execute(pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// 获取价格统计信息
    pub async fn get_price_stats(
        pool: &PgPool,
        token_symbol: &str,
        hours: i32,
    ) -> Result<Option<(Decimal, Decimal, Decimal, Decimal)>> {
        let row = sqlx::query!(
            r#"
            SELECT 
                MIN(price_usd) as min_price,
                MAX(price_usd) as max_price,
                AVG(price_usd) as avg_price,
                (SELECT price_usd FROM token_prices 
                 WHERE token_symbol = $1 AND timestamp >= NOW() - INTERVAL '%s hours'
                 ORDER BY timestamp DESC LIMIT 1) as latest_price
            FROM token_prices
            WHERE token_symbol = $1 AND timestamp >= NOW() - INTERVAL '%s hours'
            "#,
            token_symbol,
            hours.to_string(),
            hours.to_string()
        )
        .fetch_optional(pool)
        .await?;

        if let Some(row) = row {
            if let (Some(min), Some(max), Some(avg), Some(latest)) = (
                row.min_price,
                row.max_price,
                row.avg_price,
                row.latest_price,
            ) {
                Ok(Some((min, max, avg, latest)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
