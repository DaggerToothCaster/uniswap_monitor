use crate::models::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;
use bigdecimal::BigDecimal;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_tables(&self) -> Result<()> {
        // Create trading_pairs table with chain_id
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trading_pairs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain_id INTEGER NOT NULL,
            address VARCHAR(42) NOT NULL,
            token0 VARCHAR(42) NOT NULL,
            token1 VARCHAR(42) NOT NULL,
            token0_symbol VARCHAR(20),
            token1_symbol VARCHAR(20),
            token0_decimals INTEGER,
            token1_decimals INTEGER,
            token0_name VARCHAR(100),
            token1_name VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            UNIQUE(chain_id, address)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create swap_events table with chain_id
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS swap_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain_id INTEGER NOT NULL,
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
            UNIQUE(chain_id, transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create mint_events table with chain_id
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS mint_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain_id INTEGER NOT NULL,
            pair_address VARCHAR(42) NOT NULL,
            sender VARCHAR(42) NOT NULL,
            amount0 DECIMAL NOT NULL,
            amount1 DECIMAL NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            UNIQUE(chain_id, transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create burn_events table with chain_id
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS burn_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain_id INTEGER NOT NULL,
            pair_address VARCHAR(42) NOT NULL,
            sender VARCHAR(42) NOT NULL,
            amount0 DECIMAL NOT NULL,
            amount1 DECIMAL NOT NULL,
            to_address VARCHAR(42) NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            UNIQUE(chain_id, transaction_hash, log_index)
        )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_trading_pairs_chain_address ON trading_pairs(chain_id, address)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_swap_events_chain_pair ON swap_events(chain_id, pair_address)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_mint_events_chain_pair ON mint_events(chain_id, pair_address)")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_burn_events_chain_pair ON burn_events(chain_id, pair_address)")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_last_processed_block(&self, chain_id: i32) -> Result<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(block_number) FROM (
                SELECT block_number FROM trading_pairs WHERE chain_id = $1
                UNION ALL
                SELECT block_number FROM swap_events WHERE chain_id = $1
                UNION ALL
                SELECT block_number FROM mint_events WHERE chain_id = $1
                UNION ALL
                SELECT block_number FROM burn_events WHERE chain_id = $1
            ) AS all_blocks"
        )
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.map(|block| block as u64))
    }

    pub async fn insert_trading_pair(&self, pair: &TradingPair) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO trading_pairs 
            (chain_id, address, token0, token1, token0_symbol, token1_symbol, token0_decimals, token1_decimals, token0_name, token1_name, block_number, transaction_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (chain_id, address) DO NOTHING
            "#,
        )
        .bind(pair.chain_id)
        .bind(&pair.address)
        .bind(&pair.token0)
        .bind(&pair.token1)
        .bind(&pair.token0_symbol)
        .bind(&pair.token1_symbol)
        .bind(&pair.token0_decimals)
        .bind(&pair.token1_decimals)
        .bind(&pair.token0_name)
        .bind(&pair.token1_name)
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
            (chain_id, pair_address, sender, amount0_in, amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
            "#,
        )
        .bind(event.chain_id)
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0_in)
        .bind(&event.amount1_in)
        .bind(&event.amount0_out)
        .bind(&event.amount1_out)
        .bind(&event.to_address)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_all_pairs(&self, chain_id: Option<i32>) -> Result<Vec<TradingPair>> {
        let query = if let Some(chain_id) = chain_id {
            sqlx::query_as::<_, TradingPair>(
                "SELECT * FROM trading_pairs WHERE chain_id = $1 ORDER BY created_at DESC"
            )
            .bind(chain_id)
        } else {
            sqlx::query_as::<_, TradingPair>(
                "SELECT * FROM trading_pairs ORDER BY created_at DESC"
            )
        };

        let pairs = query.fetch_all(&self.pool).await?;
        Ok(pairs)
    }

    pub async fn get_token_list(&self, chain_id: Option<i32>, limit: i32) -> Result<Vec<TokenListItem>> {
        let chain_filter = if let Some(chain_id) = chain_id {
            format!("WHERE tp.chain_id = {}", chain_id)
        } else {
            String::new()
        };

        let query = format!(
            r#"
            WITH pair_stats AS (
                SELECT 
                    tp.chain_id,
                    tp.address as pair_address,
                    tp.token0_symbol,
                    tp.token1_symbol,
                    tp.token0_name,
                    tp.token1_name,
                    -- Calculate current price from latest swap
                    COALESCE(
                        (SELECT 
                            CASE 
                                WHEN amount0_out > 0 THEN amount1_in::decimal / amount0_out::decimal
                                WHEN amount1_out > 0 THEN amount0_in::decimal / amount1_out::decimal
                                ELSE 0
                            END
                        FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        ORDER BY se.timestamp DESC LIMIT 1), 0
                    ) as current_price,
                    -- Volume 1h
                    COALESCE(
                        (SELECT SUM(amount0_in + amount0_out + amount1_in + amount1_out)
                        FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        AND se.timestamp >= NOW() - INTERVAL '1 hour'), 0
                    ) as volume_1h,
                    -- Volume 24h
                    COALESCE(
                        (SELECT SUM(amount0_in + amount0_out + amount1_in + amount1_out)
                        FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        AND se.timestamp >= NOW() - INTERVAL '24 hours'), 0
                    ) as volume_24h,
                    -- Price 1h ago
                    COALESCE(
                        (SELECT 
                            CASE 
                                WHEN amount0_out > 0 THEN amount1_in::decimal / amount0_out::decimal
                                WHEN amount1_out > 0 THEN amount0_in::decimal / amount1_out::decimal
                                ELSE 0
                            END
                        FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        AND se.timestamp <= NOW() - INTERVAL '1 hour'
                        ORDER BY se.timestamp DESC LIMIT 1), 0
                    ) as price_1h_ago,
                    -- Price 24h ago
                    COALESCE(
                        (SELECT 
                            CASE 
                                WHEN amount0_out > 0 THEN amount1_in::decimal / amount0_out::decimal
                                WHEN amount1_out > 0 THEN amount0_in::decimal / amount1_out::decimal
                                ELSE 0
                            END
                        FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        AND se.timestamp <= NOW() - INTERVAL '24 hours'
                        ORDER BY se.timestamp DESC LIMIT 1), 0
                    ) as price_24h_ago,
                    tp.created_at
                FROM trading_pairs tp
                {}
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY volume_24h DESC) as rank,
                chain_id,
                CASE 
                    WHEN chain_id = 1 THEN 'Ethereum'
                    WHEN chain_id = 56 THEN 'BSC'
                    WHEN chain_id = 137 THEN 'Polygon'
                    ELSE 'Unknown'
                END as chain_name,
                pair_address,
                COALESCE(token0_symbol, 'Unknown') as token0_symbol,
                COALESCE(token1_symbol, 'Unknown') as token1_symbol,
                COALESCE(token0_name, token0_symbol, 'Unknown') as token0_name,
                COALESCE(token1_name, token1_symbol, 'Unknown') as token1_name,
                current_price as price_usd,
                CASE 
                    WHEN price_1h_ago > 0 THEN ((current_price - price_1h_ago) / price_1h_ago * 100)
                    ELSE 0
                END as price_change_1h,
                CASE 
                    WHEN price_24h_ago > 0 THEN ((current_price - price_24h_ago) / price_24h_ago * 100)
                    ELSE 0
                END as price_change_24h,
                volume_1h,
                volume_24h,
                NULL::decimal as fdv,
                NULL::decimal as market_cap,
                volume_24h as liquidity,
                NOW() as last_updated
            FROM pair_stats
            WHERE volume_24h > 0
            ORDER BY volume_24h DESC
            LIMIT $1
            "#,
            chain_filter
        );

        let rows = sqlx::query(&query)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        let mut tokens = Vec::new();
        for row in rows {
            tokens.push(TokenListItem {
                rank: row.get::<i64, _>("rank") as i32,
                chain_id: row.get("chain_id"),
                chain_name: row.get("chain_name"),
                pair_address: row.get("pair_address"),
                token0_symbol: row.get("token0_symbol"),
                token1_symbol: row.get("token1_symbol"),
                token0_name: row.get("token0_name"),
                token1_name: row.get("token1_name"),
                price_usd: row.get("price_usd"),
                price_change_1h: row.get("price_change_1h"),
                price_change_24h: row.get("price_change_24h"),
                volume_1h: row.get("volume_1h"),
                volume_24h: row.get("volume_24h"),
                fdv: row.get("fdv"),
                market_cap: row.get("market_cap"),
                liquidity: row.get("liquidity"),
                last_updated: row.get("last_updated"),
            });
        }

        Ok(tokens)
    }

    pub async fn get_chain_stats(&self) -> Result<Vec<ChainStats>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                tp.chain_id,
                CASE 
                    WHEN tp.chain_id = 1 THEN 'Ethereum'
                    WHEN tp.chain_id = 56 THEN 'BSC'
                    WHEN tp.chain_id = 137 THEN 'Polygon'
                    ELSE 'Unknown'
                END as chain_name,
                COUNT(DISTINCT tp.address) as total_pairs,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(amount0_in + amount0_out + amount1_in + amount1_out)
                    FROM swap_events se 
                    WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                    AND se.timestamp >= NOW() - INTERVAL '24 hours'), 0)
                ), 0) as total_volume_24h,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(amount0_in + amount0_out + amount1_in + amount1_out)
                    FROM swap_events se 
                    WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                    AND se.timestamp >= NOW() - INTERVAL '24 hours'), 0)
                ), 0) as total_liquidity,
                COUNT(DISTINCT CASE 
                    WHEN EXISTS(
                        SELECT 1 FROM swap_events se 
                        WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                        AND se.timestamp >= NOW() - INTERVAL '24 hours'
                    ) THEN tp.address 
                END) as active_pairs_24h
            FROM trading_pairs tp
            GROUP BY tp.chain_id
            ORDER BY total_volume_24h DESC
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(ChainStats {
                chain_id: row.get("chain_id"),
                chain_name: row.get("chain_name"),
                total_pairs: row.get("total_pairs"),
                total_volume_24h: row.get("total_volume_24h"),
                total_liquidity: row.get("total_liquidity"),
                active_pairs_24h: row.get("active_pairs_24h"),
            });
        }

        Ok(stats)
    }

    pub async fn get_kline_data(
        &self,
        pair_address: &str,
        chain_id: i32,
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
                WHERE pair_address = $1 AND chain_id = $2
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
            LIMIT $3
            "#,
            interval_seconds, interval_seconds, interval_seconds, interval_seconds * limit as i64
        );

        let rows = sqlx::query(&query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        let mut klines = Vec::new();
        for row in rows {
            klines.push(KlineData {
                timestamp: row.get("timestamp"),
                open: row.get("open"),
                high: row.get("high"),
                low: row.get("low"),
                close: row.get("close"),
                volume: row.get("volume"),
            });
        }

        Ok(klines)
    }
}
