use crate::models::*;
use anyhow::Result;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::str::FromStr;
use uuid::Uuid;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_tables(&self) -> Result<()> {
        // Create trading_pairs table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trading_pairs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address VARCHAR(42) UNIQUE NOT NULL,
            token0 VARCHAR(42) NOT NULL,
            token1 VARCHAR(42) NOT NULL,
            token0_symbol VARCHAR(20),
            token1_symbol VARCHAR(20),
            token0_decimals INTEGER,
            token1_decimals INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create swap_events table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS swap_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pair_address VARCHAR(42) NOT NULL,
            sender VARCHAR(42) NOT NULL,
            amount0_in DECIMAL NOT NULL,
            amount1_in DECIMAL NOT NULL,
            amount0_out DECIMAL NOT NULL,
            amount1_out DECIMAL NOT NULL,
            to_address VARCHAR(42) NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            UNIQUE(transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create mint_events table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS mint_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pair_address VARCHAR(42) NOT NULL,
            sender VARCHAR(42) NOT NULL,
            amount0 DECIMAL NOT NULL,
            amount1 DECIMAL NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            UNIQUE(transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create burn_events table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS burn_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pair_address VARCHAR(42) NOT NULL,
            sender VARCHAR(42) NOT NULL,
            amount0 DECIMAL NOT NULL,
            amount1 DECIMAL NOT NULL,
            to_address VARCHAR(42) NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            UNIQUE(transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_trading_pairs_address ON trading_pairs(address)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_swap_events_pair ON swap_events(pair_address)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_mint_events_pair ON mint_events(pair_address)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_burn_events_pair ON burn_events(pair_address)")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_last_processed_block(&self) -> Result<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(block_number) FROM (
                SELECT block_number FROM trading_pairs
                UNION ALL
                SELECT block_number FROM swap_events
                UNION ALL
                SELECT block_number FROM mint_events
                UNION ALL
                SELECT block_number FROM burn_events
            ) AS all_blocks",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result.map(|block| block as u64))
    }

    pub async fn insert_trading_pair(&self, pair: &TradingPair) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO trading_pairs 
            (address, token0, token1, token0_symbol, token1_symbol, token0_decimals, token1_decimals, block_number, transaction_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (address) DO NOTHING
            "#,
        )
        .bind(&pair.address)
        .bind(&pair.token0)
        .bind(&pair.token1)
        .bind(&pair.token0_symbol)
        .bind(&pair.token1_symbol)
        .bind(&pair.token0_decimals)
        .bind(&pair.token1_decimals)
        .bind(pair.block_number)
        .bind(&pair.transaction_hash)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_swap_event(&self, event: &SwapEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO swap_events 
            (pair_address, sender, amount0_in, amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING
            "#,
        )
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(event.amount0_in.to_string()) // 转换为字符串
        .bind(event.amount1_in.to_string())
        .bind(event.amount0_out.to_string())
        .bind(event.amount1_out.to_string())
        .bind(&event.to_address)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_mint_event(&self, event: &MintEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO mint_events 
            (pair_address, sender, amount0, amount1, block_number, transaction_hash, log_index, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING
            "#,
        )
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0.to_string())
        .bind(&event.amount1.to_string())
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_burn_event(&self, event: &BurnEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO burn_events 
            (pair_address, sender, amount0, amount1, to_address, block_number, transaction_hash, log_index, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING
            "#,
        )
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0.to_string())
        .bind(&event.amount1.to_string())
        .bind(&event.to_address)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
    pub async fn get_all_pairs(&self) -> Result<Vec<TradingPair>> {
        let pairs = sqlx::query_as::<_, TradingPair>(
            "SELECT * FROM trading_pairs ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(pairs)
    }

    pub async fn get_kline_data(
        &self,
        pair_address: &str,
        interval: &str,
        limit: i32,
    ) -> Result<Vec<KlineData>> {
        let interval_seconds = match interval {
            "1m" => 60,
            "5m" => 300,
            "15m" => 900,
            "1h" => 3600,
            "4h" => 14400,
            "1d" => 86400,
            _ => 3600,
        };

        let query = format!(
            r#"
            WITH price_data AS (
                SELECT 
                    date_trunc('epoch', extract(epoch from timestamp) / {} * {}) * {} as interval_start,
                    CASE 
                        WHEN amount0_out > 0 THEN amount1_in::decimal / amount0_out::decimal
                        WHEN amount1_out > 0 THEN amount0_in::decimal / amount1_out::decimal
                        ELSE 0
                    END as price,
                    (amount0_in + amount0_out + amount1_in + amount1_out) as volume
                FROM swap_events 
                WHERE pair_address = $1 
                AND timestamp >= NOW() - INTERVAL '{} seconds'
            )
            SELECT 
                to_timestamp(interval_start) as timestamp,
                FIRST_VALUE(price) OVER (PARTITION BY interval_start ORDER BY interval_start) as open,
                MAX(price) OVER (PARTITION BY interval_start) as high,
                MIN(price) OVER (PARTITION BY interval_start) as low,
                LAST_VALUE(price) OVER (PARTITION BY interval_start ORDER BY interval_start) as close,
                SUM(volume) OVER (PARTITION BY interval_start) as volume
            FROM price_data
            WHERE price > 0
            GROUP BY interval_start, price, volume
            ORDER BY interval_start DESC
            LIMIT $2
            "#,
            interval_seconds,
            interval_seconds,
            interval_seconds,
            interval_seconds * limit as i64
        );

        let rows = sqlx::query(&query)
            .bind(pair_address)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        let mut klines = Vec::new();
        for row in rows {
            klines.push(KlineData {
                timestamp: row.get("timestamp"),
                open: BigDecimal::from_str(&row.get::<String, _>("open")).unwrap_or_default(),
                high: BigDecimal::from_str(&row.get::<String, _>("high")).unwrap_or_default(),
                low: BigDecimal::from_str(&row.get::<String, _>("low")).unwrap_or_default(),
                close: BigDecimal::from_str(&row.get::<String, _>("close")).unwrap_or_default(),
                volume: BigDecimal::from_str(&row.get::<String, _>("volume")).unwrap_or_default(),
            });
        }

        Ok(klines)
    }
}
