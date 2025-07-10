use crate::types::*;
use crate::database::utils::*;
use anyhow::Result;
use sqlx::PgPool;

pub struct SystemOperations;

impl SystemOperations {
    pub async fn create_tables(pool: &PgPool) -> Result<()> {
        // Create trading_pairs table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS trading_pairs (
                chain_id INTEGER NOT NULL,
                address VARCHAR(42) NOT NULL,
                token0 VARCHAR(42) NOT NULL,
                token1 VARCHAR(42) NOT NULL,
                token0_symbol VARCHAR(20),
                token1_symbol VARCHAR(20),
                token0_decimals INTEGER,
                token1_decimals INTEGER,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chain_id, address)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create swap_events table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS swap_events (
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0_in DECIMAL(78, 0) NOT NULL,
                amount1_in DECIMAL(78, 0) NOT NULL,
                amount0_out DECIMAL(78, 0) NOT NULL,
                amount1_out DECIMAL(78, 0) NOT NULL,
                to_address VARCHAR(42) NOT NULL,
                price DECIMAL(36, 18),
                volume_usd DECIMAL(36, 18),
                trade_type VARCHAR(10),
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chain_id, transaction_hash, pair_address)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create liquidity_events table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS liquidity_events (
                chain_id INTEGER NOT NULL,
                pair_address VARCHAR(42) NOT NULL,
                sender VARCHAR(42) NOT NULL,
                amount0 DECIMAL(78, 0) NOT NULL,
                amount1 DECIMAL(78, 0) NOT NULL,
                to_address VARCHAR(42),
                event_type VARCHAR(10) NOT NULL,
                block_number BIGINT NOT NULL,
                transaction_hash VARCHAR(66) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chain_id, transaction_hash, pair_address, event_type)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create token_metadata table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS token_metadata (
                chain_id INTEGER NOT NULL,
                address VARCHAR(42) NOT NULL,
                name VARCHAR(100),
                symbol VARCHAR(20),
                decimals INTEGER,
                total_supply DECIMAL(78, 0),
                description TEXT,
                website VARCHAR(255),
                twitter VARCHAR(255),
                telegram VARCHAR(255),
                discord VARCHAR(255),
                logo_url VARCHAR(500),
                is_verified BOOLEAN DEFAULT FALSE,
                verification_level INTEGER DEFAULT 0,
                tags TEXT[],
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chain_id, address)
            )
            "#
        )
        .execute(pool)
        .await?;

        // Create last_processed_blocks table
        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS last_processed_blocks (
                chain_id INTEGER NOT NULL,
                contract_type VARCHAR(50) NOT NULL,
                last_block_number BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (chain_id, contract_type)
            )
            "#
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn create_indexes(pool: &PgPool) -> Result<(), sqlx::Error> {
        // Indexes for trading_pairs
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_trading_pairs_timestamp ON trading_pairs(timestamp DESC)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_trading_pairs_tokens ON trading_pairs(token0, token1)")
            .execute(pool).await?;

        // Indexes for swap_events
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_swap_events_pair ON swap_events(chain_id, pair_address)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp DESC)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_swap_events_sender ON swap_events(sender)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_swap_events_to ON swap_events(to_address)")
            .execute(pool).await?;

        // Indexes for liquidity_events
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_liquidity_events_pair ON liquidity_events(chain_id, pair_address)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_liquidity_events_timestamp ON liquidity_events(timestamp DESC)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_liquidity_events_sender ON liquidity_events(sender)")
            .execute(pool).await?;

        // Indexes for token_metadata
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol ON token_metadata(symbol)")
            .execute(pool).await?;
        sqlx::query!("CREATE INDEX IF NOT EXISTS idx_token_metadata_verified ON token_metadata(is_verified)")
            .execute(pool).await?;

        Ok(())
    }

    pub async fn create_views(pool: &PgPool) -> Result<(), sqlx::Error> {
        // Create a view for token statistics
        sqlx::query!(
            r#"
            CREATE OR REPLACE VIEW token_stats AS
            SELECT 
                tp.chain_id,
                COALESCE(tp.token0, tp.token1) as token_address,
                COALESCE(tp.token0_symbol, tp.token1_symbol) as symbol,
                COUNT(DISTINCT tp.address) as pair_count,
                COUNT(se.transaction_hash) as swap_count,
                SUM(COALESCE(se.volume_usd, 0)) as total_volume,
                AVG(COALESCE(se.price, 0)) as avg_price,
                MAX(se.timestamp) as last_trade
            FROM trading_pairs tp
            LEFT JOIN swap_events se ON tp.chain_id = se.chain_id AND tp.address = se.pair_address
            GROUP BY tp.chain_id, COALESCE(tp.token0, tp.token1), COALESCE(tp.token0_symbol, tp.token1_symbol)
            "#
        )
        .execute(pool)
        .await?;

        // Create a view for pair statistics
        sqlx::query!(
            r#"
            CREATE OR REPLACE VIEW pair_stats AS
            SELECT 
                tp.chain_id,
                tp.address as pair_address,
                tp.token0_symbol,
                tp.token1_symbol,
                COUNT(se.transaction_hash) as swap_count,
                SUM(COALESCE(se.volume_usd, 0)) as total_volume,
                AVG(COALESCE(se.price, 0)) as avg_price,
                MIN(se.timestamp) as first_trade,
                MAX(se.timestamp) as last_trade
            FROM trading_pairs tp
            LEFT JOIN swap_events se ON tp.chain_id = se.chain_id AND tp.address = se.pair_address
            GROUP BY tp.chain_id, tp.address, tp.token0_symbol, tp.token1_symbol
            "#
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn health_check(pool: &PgPool) -> Result<bool, sqlx::Error> {
        sqlx::query!("SELECT 1 as health_check")
            .fetch_one(pool)
            .await?;
        Ok(true)
    }

    pub async fn vacuum_analyze(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!("VACUUM ANALYZE trading_pairs")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM ANALYZE swap_events")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM ANALYZE liquidity_events")
            .execute(pool)
            .await?;
        sqlx::query!("VACUUM ANALYZE token_metadata")
            .execute(pool)
            .await?;
        Ok(())
    }
}
