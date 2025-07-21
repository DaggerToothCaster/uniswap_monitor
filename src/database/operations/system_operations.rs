use crate::types::*;
use anyhow::Result;
use sqlx::PgPool;
use tracing::debug;

pub struct SystemOperations;

impl SystemOperations {
    pub async fn create_tables(pool: &PgPool) -> Result<()> {
        // Create trading_pairs table
        // token0地址字母序小于token1
        sqlx::query!(
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
                token0_name VARCHAR(20),
                token1_name VARCHAR(20),
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT valid_token_pair CHECK (token0 < token1),
                UNIQUE (chain_id, address)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create swap_events table with foreign key
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS swap_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0_in DECIMAL(78, 0) NOT NULL,
                amount1_in DECIMAL(78, 0) NOT NULL,
                amount0_out DECIMAL(78, 0) NOT NULL,
                amount1_out DECIMAL(78, 0) NOT NULL,
                to_address VARCHAR(42) NOT NULL,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                log_index INTEGER NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                UNIQUE(chain_id, transaction_hash, log_index)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create liquidity_events table with improved schema
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS liquidity_events (
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0 DECIMAL(78, 0) NOT NULL,
                amount1 DECIMAL(78, 0) NOT NULL,
                to_address VARCHAR(42) NOT NULL,
                event_type VARCHAR(10) NOT NULL,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (chain_id, pair_address) 
                REFERENCES trading_pairs(chain_id, address)
                ON DELETE CASCADE,
                UNIQUE (chain_id, transaction_hash, event_type)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create token_metadata table with improved constraints
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS token_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                chain_id INTEGER NOT NULL,
                address VARCHAR(42) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(100) NOT NULL,
                decimals INTEGER NOT NULL,
                description TEXT,
                website_url VARCHAR(500),
                logo_url VARCHAR(500),
                twitter_url VARCHAR(500),
                telegram_url VARCHAR(500),
                discord_url VARCHAR(500),
                github_url VARCHAR(500),
                explorer_url VARCHAR(500),
                coingecko_id VARCHAR(100),
                coinmarketcap_id VARCHAR(100),
                total_supply DECIMAL,
                max_supply DECIMAL,
                is_verified BOOLEAN DEFAULT FALSE,
                tags JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(chain_id, address)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create last_processed_blocks table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS last_processed_blocks (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                chain_id INTEGER NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                last_block_number BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT valid_block_number CHECK (last_block_number >= 0),
                UNIQUE(chain_id, event_type)
            )
            "#
        )
        .execute(pool)
        .await?;

        // 添加 burn_events 表
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS burn_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0 DECIMAL(78, 0) NOT NULL,
                amount1 DECIMAL(78, 0) NOT NULL,
                to_address VARCHAR(42) NOT NULL,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                log_index INTEGER NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(chain_id, transaction_hash, log_index)
            )
            "#
        )
        .execute(pool)
        .await?;

        // 添加 mint_events 表
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS mint_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0 DECIMAL(78, 0) NOT NULL,
                amount1 DECIMAL(78, 0) NOT NULL,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                log_index INTEGER NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(chain_id, transaction_hash, log_index)
            )
            "#
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn create_indexes(pool: &PgPool) -> Result<()> {
        // Indexes for trading_pairs
        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_trading_pairs_timestamp ON trading_pairs(created_at DESC)"
        ).execute(pool).await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_trading_pairs_tokens ON trading_pairs(chain_id, token0, token1)"
        ).execute(pool).await?;

        // Indexes for swap_events
        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_swap_events_pair ON swap_events(chain_id, pair_address)"
        )
        .execute(pool)
        .await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_swap_events_pair_timestamp ON swap_events(chain_id, pair_address, timestamp DESC)"
        ).execute(pool).await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_swap_events_sender ON swap_events(chain_id, sender)"
        )
        .execute(pool)
        .await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_swap_events_to ON swap_events(chain_id, to_address)"
        )
        .execute(pool)
        .await?;

        // Indexes for liquidity_events
        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_liquidity_events_pair ON liquidity_events(chain_id, pair_address)"
        ).execute(pool).await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_liquidity_events_pair_timestamp ON liquidity_events(chain_id, pair_address, timestamp DESC)"
        ).execute(pool).await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_liquidity_events_sender ON liquidity_events(chain_id, sender)"
        ).execute(pool).await?;

        // Indexes for token_metadata
        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol ON token_metadata(chain_id, symbol)"
        ).execute(pool).await?;

        sqlx::query!(
            "CREATE INDEX IF NOT EXISTS idx_token_metadata_verified ON token_metadata(chain_id, is_verified)"
        ).execute(pool).await?;

        Ok(())
    }

    pub async fn create_views(pool: &PgPool) -> Result<(), sqlx::Error> {
        // Create materialized view for token statistics
        sqlx::query(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS token_stats_mv AS 
         SELECT
             tp.chain_id, 
             COALESCE(tp.token0, tp.token1) as token_address, 
             COALESCE(tp.token0_symbol, tp.token1_symbol) as symbol, 
             COUNT(DISTINCT tp.address) as pair_count, 
             COUNT(se.transaction_hash) as swap_count, 
             SUM(COALESCE(se.volume_usd, 0)) as total_volume, 
             AVG(COALESCE(se.price, 0)) as avg_price, 
             MAX(se.created_at) as last_trade 
         FROM trading_pairs tp 
         LEFT JOIN swap_events se ON tp.chain_id = se.chain_id AND tp.address = se.pair_address 
         GROUP BY tp.chain_id, COALESCE(tp.token0, tp.token1), COALESCE(tp.token0_symbol, tp.token1_symbol)"
    )
    .execute(pool)
    .await?;

        // Create materialized view for pair statistics
        sqlx::query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS pair_stats_mv AS 
         SELECT 
             tp.chain_id, 
             tp.address as pair_address, 
             tp.token0_symbol, 
             tp.token1_symbol, 
             COUNT(se.transaction_hash) as swap_count, 
             SUM(COALESCE(se.volume_usd, 0)) as total_volume, 
             AVG(COALESCE(se.price, 0)) as avg_price, 
             MIN(se.created_at) as first_trade, 
             MAX(se.created_at) as last_trade 
         FROM trading_pairs tp 
         LEFT JOIN swap_events se ON tp.chain_id = se.chain_id AND tp.address = se.pair_address 
         GROUP BY tp.chain_id, tp.address, tp.token0_symbol, tp.token1_symbol",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn health_check(pool: &PgPool) -> Result<bool> {
        sqlx::query!("SELECT 1 as health_check")
            .fetch_one(pool)
            .await?;
        Ok(true)
    }

    pub async fn vacuum_analyze(pool: &PgPool) -> Result<()> {
        sqlx::query!("VACUUM (ANALYZE, VERBOSE) trading_pairs")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM (ANALYZE, VERBOSE) swap_events")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM (ANALYZE, VERBOSE) liquidity_events")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM (ANALYZE, VERBOSE) token_metadata")
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn refresh_materialized_views(pool: &PgPool) -> Result<()> {
        sqlx::query!("REFRESH MATERIALIZED VIEW CONCURRENTLY token_stats_mv")
            .execute(pool)
            .await?;
        sqlx::query!("REFRESH MATERIALIZED VIEW CONCURRENTLY pair_stats_mv")
            .execute(pool)
            .await?;
        Ok(())
    }
}
