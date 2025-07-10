use crate::types::*;
use crate::database::utils::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row, postgres::PgRow};
use uuid::Uuid;
use rust_decimal::Decimal;

// 事件类型常量
pub const EVENT_TYPE_FACTORY: &str = "factory";
pub const EVENT_TYPE_SWAP: &str = "swap";
pub const EVENT_TYPE_UNIFIED: &str = "unified";

pub async fn create_tables(pool: &PgPool) -> Result<()> {
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
    .execute(pool)
    .await?;

    // Create token_metadata table
    sqlx::query(
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
        "#,
    )
    .execute(pool)
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
    .execute(pool)
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
    .execute(pool)
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
    .execute(pool)
    .await?;

    // Create last_processed_blocks table with event_type
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS last_processed_blocks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain_id INTEGER NOT NULL,
            event_type VARCHAR(20) NOT NULL DEFAULT 'unified',
            last_block_number BIGINT NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(chain_id, event_type)
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create indexes
    create_indexes(pool).await?;

    // Create processing status view
    create_processing_status_view(pool).await?;

    Ok(())
}

async fn create_indexes(pool: &PgPool) -> Result<()> {
    let indexes = vec![
        "CREATE INDEX IF NOT EXISTS idx_trading_pairs_chain_address ON trading_pairs(chain_id, address)",
        "CREATE INDEX IF NOT EXISTS idx_token_metadata_chain_address ON token_metadata(chain_id, address)",
        "CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol ON token_metadata(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_swap_events_chain_pair ON swap_events(chain_id, pair_address)",
        "CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_swap_events_sender ON swap_events(sender)",
        "CREATE INDEX IF NOT EXISTS idx_swap_events_to_address ON swap_events(to_address)",
        "CREATE INDEX IF NOT EXISTS idx_mint_events_chain_pair ON mint_events(chain_id, pair_address)",
        "CREATE INDEX IF NOT EXISTS idx_mint_events_sender ON mint_events(sender)",
        "CREATE INDEX IF NOT EXISTS idx_burn_events_chain_pair ON burn_events(chain_id, pair_address)",
        "CREATE INDEX IF NOT EXISTS idx_burn_events_sender ON burn_events(sender)",
        "CREATE INDEX IF NOT EXISTS idx_burn_events_to_address ON burn_events(to_address)",
        "CREATE INDEX IF NOT EXISTS idx_last_processed_blocks_chain_event ON last_processed_blocks(chain_id, event_type)",
    ];

    for index_sql in indexes {
        sqlx::query(index_sql).execute(pool).await?;
    }

    Ok(())
}

async fn create_processing_status_view(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE OR REPLACE VIEW processing_status AS
        SELECT 
            chain_id,
            CASE 
                WHEN chain_id = 1 THEN 'Ethereum'
                WHEN chain_id = 56 THEN 'BSC'
                WHEN chain_id = 137 THEN 'Polygon'
                WHEN chain_id = 42161 THEN 'Arbitrum'
                ELSE 'Unknown'
            END as chain_name,
            MAX(CASE WHEN event_type = 'factory' THEN last_block_number END) as factory_block,
            MAX(CASE WHEN event_type = 'swap' THEN last_block_number END) as swap_block,
            MIN(CASE WHEN event_type IN ('factory', 'swap') THEN last_block_number END) as min_processed_block,
            MAX(CASE WHEN event_type IN ('factory', 'swap') THEN last_block_number END) as max_processed_block,
            MAX(CASE WHEN event_type = 'factory' THEN updated_at END) as factory_updated_at,
            MAX(CASE WHEN event_type = 'swap' THEN updated_at END) as swap_updated_at
        FROM last_processed_blocks 
        WHERE event_type IN ('factory', 'swap')
        GROUP BY chain_id
        ORDER BY chain_id
        "#
    )
    .execute(pool)
    .await?;

    Ok(())
}

// Trading Pairs operations
pub async fn insert_trading_pair(pool: &PgPool, pair: &TradingPair) -> Result<()> {
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
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_all_pairs(pool: &PgPool, chain_id: Option<i32>) -> Result<Vec<TradingPair>> {
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

    let pairs = query.fetch_all(pool).await?;
    Ok(pairs)
}

// Event operations
pub async fn insert_swap_event(pool: &PgPool, event: &SwapEvent) -> Result<()> {
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
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_mint_event(pool: &PgPool, event: &MintEvent) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO mint_events 
        (chain_id, pair_address, sender, amount0, amount1, block_number, transaction_hash, log_index, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(event.chain_id)
    .bind(&event.pair_address)
    .bind(&event.sender)
    .bind(&event.amount0)
    .bind(&event.amount1)
    .bind(event.block_number)
    .bind(&event.transaction_hash)
    .bind(event.log_index)
    .bind(event.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_burn_event(pool: &PgPool, event: &BurnEvent) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO burn_events 
        (chain_id, pair_address, sender, amount0, amount1, to_address, block_number, transaction_hash, log_index, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(event.chain_id)
    .bind(&event.pair_address)
    .bind(&event.sender)
    .bind(&event.amount0)
    .bind(&event.amount1)
    .bind(&event.to_address)
    .bind(event.block_number)
    .bind(&event.transaction_hash)
    .bind(event.log_index)
    .bind(event.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}

// 修改后的区块跟踪操作 - 支持事件类型
pub async fn get_last_processed_block(pool: &PgPool, chain_id: i32, event_type: &str) -> Result<u64> {
    let result = sqlx::query_scalar::<_, i64>(
        "SELECT last_block_number FROM last_processed_blocks WHERE chain_id = $1 AND event_type = $2"
    )
    .bind(chain_id)
    .bind(event_type)
    .fetch_optional(pool)
    .await?;

    Ok(result.unwrap_or(0) as u64)
}

pub async fn update_last_processed_block(pool: &PgPool, chain_id: i32, event_type: &str, block_number: u64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id, event_type) 
        DO UPDATE SET 
            last_block_number = $3,
            updated_at = NOW()
        "#
    )
    .bind(chain_id)
    .bind(event_type)
    .bind(block_number as i64)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn initialize_last_processed_block(pool: &PgPool, chain_id: i32, event_type: &str, start_block: u64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id, event_type) DO NOTHING
        "#
    )
    .bind(chain_id)
    .bind(event_type)
    .bind(start_block as i64)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_all_last_processed_blocks(pool: &PgPool) -> Result<Vec<LastProcessedBlock>> {
    let blocks = sqlx::query_as::<_, LastProcessedBlock>(
        "SELECT * FROM last_processed_blocks ORDER BY chain_id, event_type"
    )
    .fetch_all(pool)
    .await?;

    Ok(blocks)
}

// 新增：获取处理状态视图
pub async fn get_processing_status(pool: &PgPool) -> Result<Vec<ProcessingStatus>> {
    let status = sqlx::query_as::<_, ProcessingStatus>(
        "SELECT * FROM processing_status ORDER BY chain_id"
    )
    .fetch_all(pool)
    .await?;

    Ok(status)
}

// 优化后的K线数据查询 - 确保开盘价连续性
pub async fn get_kline_data(
    pool: &PgPool,
    pair_address: &str,
    chain_id: i32,
    interval: &str,
    limit: i32,
) -> Result<Vec<KlineData>> {
    // 根据不同的时间区间使用不同的查询
    let (query, _interval_param) = match interval {
        "1m" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('minute', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '1 day'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    -- 确保最高价不低于开盘价和收盘价
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    -- 确保最低价不高于开盘价和收盘价
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "1 day"
        ),
        "5m" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 5) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 5) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 5) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '3 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "3 days"
        ),
        "15m" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 15) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 15) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 15) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '7 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "7 days"
        ),
        "30m" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 30) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 30) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 30) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '14 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "14 days"
        ),
        "1h" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('hour', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '30 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "30 days"
        ),
        "4h" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('hour', timestamp) - INTERVAL '1 hour' * (EXTRACT(hour FROM timestamp)::int % 4) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) - INTERVAL '1 hour' * (EXTRACT(hour FROM timestamp)::int % 4) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) - INTERVAL '1 hour' * (EXTRACT(hour FROM timestamp)::int % 4) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '90 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "90 days"
        ),
        "1d" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('day', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('day', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('day', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '1 year'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "1 year"
        ),
        "1w" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('week', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('week', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('week', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '2 years'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "2 years"
        ),
        "1M" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('month', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('month', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('month', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '5 years'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "5 years"
        ),
        "1y" => (
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('year', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('year', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('year', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "all time"
        ),
        _ => (
            // 默认使用1小时
            r#"
            WITH time_series AS (
                SELECT 
                    date_trunc('hour', timestamp) as time_bucket,
                    CASE 
                        WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                        WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                        ELSE 0
                    END as price,
                    CASE 
                        WHEN amount0_in > 0 THEN amount0_in
                        WHEN amount1_in > 0 THEN amount1_in  
                        ELSE 0
                    END as volume,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) ORDER BY timestamp ASC) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', timestamp) ORDER BY timestamp DESC) as rn_last
                FROM swap_events 
                WHERE pair_address = $1 AND chain_id = $2
                AND timestamp >= NOW() - INTERVAL '30 days'
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR 
                    (amount1_in > 0 AND amount0_out > 0)
                )
            ),
            kline_raw AS (
                SELECT 
                    time_bucket,
                    MAX(CASE WHEN rn_first = 1 THEN price END) as first_price,
                    MAX(price) as high_price,
                    MIN(price) as low_price,
                    MAX(CASE WHEN rn_last = 1 THEN price END) as close_price,
                    SUM(volume) as total_volume,
                    COUNT(*) as trade_count
                FROM time_series
                WHERE price > 0
                GROUP BY time_bucket
            ),
            kline_with_continuity AS (
                SELECT 
                    time_bucket,
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    GREATEST(
                        high_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as high_price,
                    LEAST(
                        low_price,
                        CASE 
                            WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                            THEN first_price 
                            ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                        END,
                        close_price
                    ) as low_price,
                    close_price,
                    total_volume,
                    trade_count
                FROM kline_raw
                WHERE first_price IS NOT NULL AND close_price IS NOT NULL
            )
            SELECT 
                time_bucket as timestamp,
                COALESCE(open_price, 0) as open,
                COALESCE(high_price, 0) as high,
                COALESCE(low_price, 0) as low,
                COALESCE(close_price, 0) as close,
                COALESCE(total_volume, 0) as volume,
                COALESCE(trade_count, 0) as trade_count
            FROM kline_with_continuity
            ORDER BY time_bucket DESC
            LIMIT $3
            "#,
            "30 days"
        )
    };

    let rows = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut klines = Vec::new();
    for row in rows {
        klines.push(KlineData {
            timestamp: safe_get_datetime(&row, "timestamp"),
            open: safe_get_decimal(&row, "open"),
            high: safe_get_decimal(&row, "high"),
            low: safe_get_decimal(&row, "low"),
            close: safe_get_decimal(&row, "close"),
            volume: safe_get_decimal(&row, "volume"),
            trade_count: safe_get_i64(&row, "trade_count"),
        });
    }

    Ok(klines)
}

// 修复后的分时图数据查询 - 同样修复价格计算逻辑
pub async fn get_timeseries_data(
    pool: &PgPool,
    pair_address: &str,
    chain_id: i32,
    hours: i32,
) -> Result<Vec<TimeSeriesData>> {
    let query = r#"
        SELECT 
            timestamp,
            CASE 
                WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                ELSE 0
            END as price,
            CASE 
                WHEN amount0_in > 0 THEN amount0_in
                WHEN amount1_in > 0 THEN amount1_in  
                ELSE 0
            END as volume
        FROM swap_events 
        WHERE pair_address = $1 AND chain_id = $2
        AND timestamp >= NOW() - INTERVAL '1 hour' * $3
        AND (
            (amount0_in > 0 AND amount1_out > 0) OR 
            (amount1_in > 0 AND amount0_out > 0)
        )
        AND (
            CASE 
                WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                ELSE 0
            END
        ) > 0
        ORDER BY timestamp ASC
    "#;

    let rows = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .bind(hours)
        .fetch_all(pool)
        .await?;

    let mut timeseries = Vec::new();
    for row in rows {
        timeseries.push(TimeSeriesData {
            timestamp: safe_get_datetime(&row, "timestamp"),
            price: safe_get_decimal(&row, "price"),
            volume: safe_get_decimal(&row, "volume"),
        });
    }

    Ok(timeseries)
}

// 修复后的交易记录查询 - 同样修复价格计算逻辑
pub async fn get_pair_trades(
    pool: &PgPool,
    pair_address: &str,
    chain_id: i32,
    limit: i32,
    offset: i32,
) -> Result<Vec<TradeRecord>> {
    let query = r#"
        SELECT 
            se.id,
            se.chain_id,
            se.pair_address,
            tp.token0_symbol,
            tp.token1_symbol,
            se.transaction_hash,
            se.sender,
            se.to_address,
            se.amount0_in,
            se.amount1_in,
            se.amount0_out,
            se.amount1_out,
            CASE 
                WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN se.amount0_in / se.amount1_out
                WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN se.amount0_out / se.amount1_in
                ELSE 0
            END as price,
            CASE 
                WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN 'buy'  -- 用token0买token1
                WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN 'sell' -- 用token1买token0
                ELSE 'unknown'
            END as trade_type,
            se.block_number,
            se.timestamp
        FROM swap_events se
        LEFT JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
        WHERE se.pair_address = $1 AND se.chain_id = $2
        ORDER BY se.timestamp DESC
        LIMIT $3 OFFSET $4
    "#;

    let rows = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let mut trades = Vec::new();
    for row in rows {
        trades.push(TradeRecord {
            id: safe_get_uuid(&row, "id"),
            chain_id: safe_get_i32(&row, "chain_id"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_optional_string(&row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(&row, "token1_symbol"),
            transaction_hash: safe_get_string(&row, "transaction_hash"),
            sender: safe_get_string(&row, "sender"),
            to_address: safe_get_string(&row, "to_address"),
            amount0_in: safe_get_decimal(&row, "amount0_in"),
            amount1_in: safe_get_decimal(&row, "amount1_in"),
            amount0_out: safe_get_decimal(&row, "amount0_out"),
            amount1_out: safe_get_decimal(&row, "amount1_out"),
            price: safe_get_decimal(&row, "price"),
            trade_type: safe_get_string(&row, "trade_type"),
            volume_usd: None,
            block_number: safe_get_i64(&row, "block_number"),
            timestamp: safe_get_datetime(&row, "timestamp"),
        });
    }

    Ok(trades)
}

// Token metadata operations
pub async fn create_token_metadata(pool: &PgPool, metadata: &CreateTokenMetadata) -> Result<TokenMetadata> {
    let tags_json = metadata.tags.as_ref().map(|tags| serde_json::to_value(tags).unwrap());
    
    let row = sqlx::query(
        r#"
        INSERT INTO token_metadata 
        (chain_id, address, symbol, name, decimals, description, website_url, logo_url, 
         twitter_url, telegram_url, discord_url, github_url, explorer_url, coingecko_id, 
         coinmarketcap_id, total_supply, max_supply, tags)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        RETURNING *
        "#,
    )
    .bind(metadata.chain_id)
    .bind(&metadata.address)
    .bind(&metadata.symbol)
    .bind(&metadata.name)
    .bind(metadata.decimals)
    .bind(&metadata.description)
    .bind(&metadata.website_url)
    .bind(&metadata.logo_url)
    .bind(&metadata.twitter_url)
    .bind(&metadata.telegram_url)
    .bind(&metadata.discord_url)
    .bind(&metadata.github_url)
    .bind(&metadata.explorer_url)
    .bind(&metadata.coingecko_id)
    .bind(&metadata.coinmarketcap_id)
    .bind(&metadata.total_supply)
    .bind(&metadata.max_supply)
    .bind(&tags_json)
    .fetch_one(pool)
    .await?;

    Ok(row_to_token_metadata(row)?)
}

pub async fn get_token_metadata(pool: &PgPool, chain_id: i32, address: &str) -> Result<Option<TokenMetadata>> {
    let row = sqlx::query(
        "SELECT * FROM token_metadata WHERE chain_id = $1 AND address = $2"
    )
    .bind(chain_id)
    .bind(address)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = row {
        Ok(Some(row_to_token_metadata(row)?))
    } else {
        Ok(None)
    }
}

fn row_to_token_metadata(row: sqlx::postgres::PgRow) -> Result<TokenMetadata> {
    let tags_json: Option<serde_json::Value> = row.try_get("tags").ok().flatten();
    let tags = tags_json.and_then(|v| serde_json::from_value(v).ok());

    Ok(TokenMetadata {
        id: safe_get_uuid(&row, "id"),
        chain_id: safe_get_i32(&row, "chain_id"),
        address: safe_get_string(&row, "address"),
        symbol: safe_get_string(&row, "symbol"),
        name: safe_get_string(&row, "name"),
        decimals: safe_get_i32(&row, "decimals"),
        description: safe_get_optional_string(&row, "description"),
        website_url: safe_get_optional_string(&row, "website_url"),
        logo_url: safe_get_optional_string(&row, "logo_url"),
        twitter_url: safe_get_optional_string(&row, "twitter_url"),
        telegram_url: safe_get_optional_string(&row, "telegram_url"),
        discord_url: safe_get_optional_string(&row, "discord_url"),
        github_url: safe_get_optional_string(&row, "github_url"),
        explorer_url: safe_get_optional_string(&row, "explorer_url"),
        coingecko_id: safe_get_optional_string(&row, "coingecko_id"),
        coinmarketcap_id: safe_get_optional_string(&row, "coinmarketcap_id"),
        total_supply: safe_get_optional_decimal(&row, "total_supply"),
        max_supply: safe_get_optional_decimal(&row, "max_supply"),
        is_verified: safe_get_bool(&row, "is_verified"),
        tags,
        created_at: safe_get_datetime(&row, "created_at"),
        updated_at: safe_get_datetime(&row, "updated_at"),
    })
}
