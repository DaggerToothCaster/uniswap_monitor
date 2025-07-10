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

pub async fn get_pair_detail(pool: &PgPool, pair_address: &str, chain_id: i32) -> Result<Option<PairDetail>> {
    let query = r#"
        WITH pair_stats AS (
            SELECT 
                tp.*,
                COALESCE(
                    (SELECT 
                        CASE 
                            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                            ELSE 0
                        END
                     FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND (
                         (amount0_in > 0 AND amount1_out > 0) OR 
                         (amount1_in > 0 AND amount0_out > 0)
                     )
                     ORDER BY timestamp DESC 
                     LIMIT 1), 0
                ) as current_price,
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN amount0_in > 0 THEN amount0_in
                            WHEN amount1_in > 0 THEN amount1_in  
                            ELSE 0
                        END
                    ) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as volume_24h,
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN amount0_in > 0 THEN amount0_in
                            WHEN amount1_in > 0 THEN amount1_in  
                            ELSE 0
                        END
                    ) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '7 days'), 0
                ) as volume_7d,
                COALESCE(
                    (SELECT COUNT(*) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as tx_count_24h,
                COALESCE(
                    (SELECT COUNT(*) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '7 days'), 0
                ) as tx_count_7d
            FROM trading_pairs tp
            WHERE tp.address = $1 AND tp.chain_id = $2
        )
        SELECT 
            address as pair_address,
            chain_id,
            token0,
            token1,
            token0_symbol,
            token1_symbol,
            token0_name,
            token1_name,
            token0_decimals,
            token1_decimals,
            current_price,
            volume_24h,
            volume_7d,
            0 as liquidity,
            0 as price_change_24h,
            0 as price_change_7d,
            tx_count_24h,
            tx_count_7d,
            created_at
        FROM pair_stats
    "#;

    let row = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        Ok(Some(PairDetail {
            pair_address: safe_get_string(&row, "pair_address"),
            chain_id: safe_get_i32(&row, "chain_id"),
            token0: safe_get_string(&row, "token0"),
            token1: safe_get_string(&row, "token1"),
            token0_symbol: safe_get_optional_string(&row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(&row, "token1_symbol"),
            token0_name: safe_get_optional_string(&row, "token0_name"),
            token1_name: safe_get_optional_string(&row, "token1_name"),
            token0_decimals: safe_get_optional_i32(&row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(&row, "token1_decimals"),
            current_price: safe_get_decimal(&row, "current_price"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            volume_7d: safe_get_decimal(&row, "volume_7d"),
            liquidity: safe_get_decimal(&row, "liquidity"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            price_change_7d: safe_get_decimal(&row, "price_change_7d"),
            tx_count_24h: safe_get_i64(&row, "tx_count_24h"),
            tx_count_7d: safe_get_i64(&row, "tx_count_7d"),
            created_at: safe_get_datetime(&row, "created_at"),
        }))
    } else {
        Ok(None)
    }
}

// Event operations
pub async fn insert_swap_event(pool: &PgPool, event: &SwapEvent) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO swap_events 
        (chain_id, pair_address, sender, amount0_in, amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index,
        amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index, timestamp)
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

// 修复后的K线数据查询 - 正确处理价格计算逻辑
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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
            WHERE time_bucket >= NOW() - INTERVAL '5 years'
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
                    -- 开盘价：第一个K线使用实际第一笔交易价格，后续使用前一个K线的收盘价
                    CASE 
                        WHEN LAG(close_price) OVER (ORDER BY time_bucket) IS NULL 
                        THEN first_price 
                        ELSE LAG(close_price) OVER (ORDER BY time_bucket)
                    END as open_price,
                    high_price,
                    low_price,
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

// 修复后的交易记录查询 - 包含代币精度信息
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
            tp.token0_decimals,
            tp.token1_decimals,
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
            token0_decimals: safe_get_optional_i32(&row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(&row, "token1_decimals"),
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

// 新增：获取流动性事件
pub async fn get_pair_liquidity_events(
    pool: &PgPool,
    pair_address: &str,
    chain_id: i32,
    limit: i32,
    offset: i32,
) -> Result<Vec<LiquidityRecord>> {
    let query = r#"
        WITH liquidity_events AS (
            SELECT 
                me.id,
                me.chain_id,
                me.pair_address,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                me.transaction_hash,
                me.sender,
                NULL as to_address,
                me.amount0,
                me.amount1,
                'mint' as liquidity_type,
                me.block_number,
                me.timestamp
            FROM mint_events me
            LEFT JOIN trading_pairs tp ON tp.address = me.pair_address AND tp.chain_id = me.chain_id
            WHERE me.pair_address = $1 AND me.chain_id = $2
            
            UNION ALL
            
            SELECT 
                be.id,
                be.chain_id,
                be.pair_address,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                be.transaction_hash,
                be.sender,
                be.to_address,
                be.amount0,
                be.amount1,
                'burn' as liquidity_type,
                be.block_number,
                be.timestamp
            FROM burn_events be
            LEFT JOIN trading_pairs tp ON tp.address = be.pair_address AND tp.chain_id = be.chain_id
            WHERE be.pair_address = $1 AND be.chain_id = $2
        )
        SELECT * FROM liquidity_events
        ORDER BY timestamp DESC
        LIMIT $3 OFFSET $4
    "#;

    let rows = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let mut liquidity_records = Vec::new();
    for row in rows {
        liquidity_records.push(LiquidityRecord {
            id: safe_get_uuid(&row, "id"),
            chain_id: safe_get_i32(&row, "chain_id"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_optional_string(&row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(&row, "token1_symbol"),
            token0_decimals: safe_get_optional_i32(&row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(&row, "token1_decimals"),
            transaction_hash: safe_get_string(&row, "transaction_hash"),
            sender: safe_get_string(&row, "sender"),
            to_address: safe_get_optional_string(&row, "to_address"),
            amount0: safe_get_decimal(&row, "amount0"),
            amount1: safe_get_decimal(&row, "amount1"),
            liquidity_type: safe_get_string(&row, "liquidity_type"),
            value_usd: None,
            block_number: safe_get_i64(&row, "block_number"),
            timestamp: safe_get_datetime(&row, "timestamp"),
        });
    }

    Ok(liquidity_records)
}

// 新增：获取交易对统计信息
pub async fn get_pair_stats(
    pool: &PgPool,
    pair_address: &str,
    chain_id: i32,
) -> Result<Option<PairStats>> {
    let query = r#"
        WITH pair_info AS (
            SELECT 
                tp.address as pair_address,
                tp.chain_id,
                tp.token0_symbol,
                tp.token1_symbol,
                -- 当前价格
                COALESCE(
                    (SELECT 
                        CASE 
                            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                            ELSE 0
                        END
                     FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND (
                         (amount0_in > 0 AND amount1_out > 0) OR 
                         (amount1_in > 0 AND amount0_out > 0)
                     )
                     ORDER BY timestamp DESC 
                     LIMIT 1), 0
                ) as current_price,
                -- 24小时前价格
                COALESCE(
                    (SELECT 
                        CASE 
                            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                            ELSE 0
                        END
                     FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp <= NOW() - INTERVAL '24 hours'
                     AND (
                         (amount0_in > 0 AND amount1_out > 0) OR 
                         (amount1_in > 0 AND amount0_out > 0)
                     )
                     ORDER BY timestamp DESC 
                     LIMIT 1), 0
                ) as price_24h_ago,
                -- 24小时成交量
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN amount0_in > 0 THEN amount0_in
                            WHEN amount1_in > 0 THEN amount1_in  
                            ELSE 0
                        END
                    ) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as volume_24h,
                -- 24小时交易次数
                COALESCE(
                    (SELECT COUNT(*) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as tx_count_24h
            FROM trading_pairs tp
            WHERE tp.address = $1 AND tp.chain_id = $2
        )
        SELECT 
            pair_address,
            chain_id,
            COALESCE(token0_symbol, 'UNKNOWN') as token0_symbol,
            COALESCE(token1_symbol, 'UNKNOWN') as token1_symbol,
            current_price as price,
            volume_24h,
            0 as liquidity,
            CASE 
                WHEN price_24h_ago > 0 THEN 
                    ((current_price - price_24h_ago) / price_24h_ago) * 100
                ELSE 0
            END as price_change_24h,
            tx_count_24h
        FROM pair_info
    "#;

    let row = sqlx::query(query)
        .bind(pair_address)
        .bind(chain_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        Ok(Some(PairStats {
            pair_address: safe_get_string(&row, "pair_address"),
            chain_id: safe_get_i32(&row, "chain_id"),
            token0_symbol: safe_get_string(&row, "token0_symbol"),
            token1_symbol: safe_get_string(&row, "token1_symbol"),
            price: safe_get_decimal(&row, "price"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            liquidity: safe_get_decimal(&row, "liquidity"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            tx_count_24h: safe_get_i64(&row, "tx_count_24h"),
        }))
    } else {
        Ok(None)
    }
}

// Token相关操作
pub async fn get_token_list(
    pool: &PgPool,
    chain_id: Option<i32>,
    limit: i32,
    offset: i32,
    sort_by: &str,
    order: &str,
) -> Result<Vec<TokenListItem>> {
    let order_clause = match order.to_lowercase().as_str() {
        "asc" => "ASC",
        _ => "DESC",
    };

    let sort_column = match sort_by {
        "price" => "current_price",
        "volume" => "volume_24h",
        "market_cap" => "market_cap",
        "liquidity" => "total_liquidity",
        _ => "volume_24h",
    };

    let chain_filter = if let Some(chain_id) = chain_id {
        format!("WHERE tp.chain_id = {}", chain_id)
    } else {
        "".to_string()
    };

    let query = format!(
        r#"
        WITH token_stats AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(volume_24h), 0) DESC) as rank,
                tp.chain_id,
                CASE 
                    WHEN tp.chain_id = 1 THEN 'Ethereum'
                    WHEN tp.chain_id = 56 THEN 'BSC'
                    WHEN tp.chain_id = 137 THEN 'Polygon'
                    WHEN tp.chain_id = 42161 THEN 'Arbitrum'
                    ELSE 'Unknown'
                END as chain_name,
                tp.address as pair_address,
                COALESCE(tp.token0_symbol, 'UNKNOWN') as token0_symbol,
                COALESCE(tp.token1_symbol, 'UNKNOWN') as token1_symbol,
                COALESCE(tp.token0_name, 'Unknown Token') as token0_name,
                COALESCE(tp.token1_name, 'Unknown Token') as token1_name,
                tm0.logo_url as token0_logo_url,
                tm1.logo_url as token1_logo_url,
                tm0.website_url as token0_website_url,
                tm1.website_url as token1_website_url,
                tm0.explorer_url as token0_explorer_url,
                tm1.explorer_url as token1_explorer_url,
                tm0.description as token0_description,
                tm1.description as token1_description,
                tm0.tags as token0_tags,
                tm1.tags as token1_tags,
                -- 当前价格
                COALESCE(
                    (SELECT 
                        CASE 
                            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in / amount1_out
                            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out / amount1_in
                            ELSE 0
                        END
                     FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND (
                         (amount0_in > 0 AND amount1_out > 0) OR 
                         (amount1_in > 0 AND amount0_out > 0)
                     )
                     ORDER BY timestamp DESC 
                     LIMIT 1), 0
                ) as current_price,
                -- 1小时价格变化
                0 as price_change_1h,
                -- 24小时价格变化
                0 as price_change_24h,
                -- 1小时成交量
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN amount0_in > 0 THEN amount0_in
                            WHEN amount1_in > 0 THEN amount1_in  
                            ELSE 0
                        END
                    ) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '1 hour'), 0
                ) as volume_1h,
                -- 24小时成交量
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN amount0_in > 0 THEN amount0_in
                            WHEN amount1_in > 0 THEN amount1_in  
                            ELSE 0
                        END
                    ) FROM swap_events 
                     WHERE pair_address = tp.address AND chain_id = tp.chain_id
                     AND timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as volume_24h,
                0 as fdv,
                0 as market_cap,
                0 as total_liquidity,
                NOW() as last_updated
            FROM trading_pairs tp
            LEFT JOIN token_metadata tm0 ON tm0.chain_id = tp.chain_id AND tm0.address =tp.token0
            LEFT JOIN token_metadata tm1 ON tm1.chain_id = tp.chain_id AND tm1.address = tp.token1
            {}
            GROUP BY tp.chain_id, tp.address, tp.token0_symbol, tp.token1_symbol, tp.token0_name, tp.token1_name,
                     tm0.logo_url, tm1.logo_url, tm0.website_url, tm1.website_url, tm0.explorer_url, tm1.explorer_url,
                     tm0.description, tm1.description, tm0.tags, tm1.tags
        )
        SELECT * FROM token_stats
        ORDER BY {} {}
        LIMIT {} OFFSET {}
        "#,
        chain_filter, sort_column, order_clause, limit, offset
    );

    let rows = sqlx::query(&query).fetch_all(pool).await?;

    let mut tokens = Vec::new();
    for row in rows {
        let token0_tags: Option<Vec<String>> = safe_get_optional_string(&row, "token0_tags")
            .and_then(|s| serde_json::from_str(&s).ok());
        let token1_tags: Option<Vec<String>> = safe_get_optional_string(&row, "token1_tags")
            .and_then(|s| serde_json::from_str(&s).ok());

        tokens.push(TokenListItem {
            rank: safe_get_i32(&row, "rank"),
            chain_id: safe_get_i32(&row, "chain_id"),
            chain_name: safe_get_string(&row, "chain_name"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_string(&row, "token0_symbol"),
            token1_symbol: safe_get_string(&row, "token1_symbol"),
            token0_name: safe_get_string(&row, "token0_name"),
            token1_name: safe_get_string(&row, "token1_name"),
            token0_logo_url: safe_get_optional_string(&row, "token0_logo_url"),
            token1_logo_url: safe_get_optional_string(&row, "token1_logo_url"),
            token0_website_url: safe_get_optional_string(&row, "token0_website_url"),
            token1_website_url: safe_get_optional_string(&row, "token1_website_url"),
            token0_explorer_url: safe_get_optional_string(&row, "token0_explorer_url"),
            token1_explorer_url: safe_get_optional_string(&row, "token1_explorer_url"),
            token0_description: safe_get_optional_string(&row, "token0_description"),
            token1_description: safe_get_optional_string(&row, "token1_description"),
            token0_tags,
            token1_tags,
            price_usd: safe_get_decimal(&row, "current_price"),
            price_change_1h: safe_get_decimal(&row, "price_change_1h"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            volume_1h: safe_get_decimal(&row, "volume_1h"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            fdv: safe_get_optional_decimal(&row, "fdv"),
            market_cap: safe_get_optional_decimal(&row, "market_cap"),
            liquidity: safe_get_decimal(&row, "total_liquidity"),
            last_updated: safe_get_datetime(&row, "last_updated"),
        });
    }

    Ok(tokens)
}

pub async fn get_token_detail(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<Option<TokenDetail>> {
    // 获取token元数据
    let metadata = get_token_metadata(pool, chain_id, address).await?;
    
    if let Some(metadata) = metadata {
        // 获取价格信息
        let price_info = get_token_price_info(pool, chain_id, address).await?;
        
        // 获取交易对信息
        let trading_pairs = get_token_trading_pairs(pool, chain_id, address).await?;
        
        Ok(Some(TokenDetail {
            metadata,
            price_info,
            trading_pairs,
        }))
    } else {
        Ok(None)
    }
}

async fn get_token_price_info(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<Option<TokenPriceInfo>> {
    let query = r#"
        WITH token_prices AS (
            SELECT 
                -- 当前价格 (取最新的交易价格)
                COALESCE(
                    (SELECT 
                        CASE 
                            WHEN se.amount0_in > 0 AND se.amount1_out > 0 AND tp.token0 = $2 THEN se.amount0_in / se.amount1_out
                            WHEN se.amount1_in > 0 AND se.amount0_out > 0 AND tp.token1 = $2 THEN se.amount0_out / se.amount1_in
                            WHEN se.amount0_in > 0 AND se.amount1_out > 0 AND tp.token1 = $2 THEN se.amount1_out / se.amount0_in
                            WHEN se.amount1_in > 0 AND se.amount0_out > 0 AND tp.token0 = $2 THEN se.amount1_in / se.amount0_out
                            ELSE 0
                        END
                     FROM swap_events se
                     JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
                     WHERE se.chain_id = $1 AND (tp.token0 = $2 OR tp.token1 = $2)
                     AND (
                         (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                         (se.amount1_in > 0 AND se.amount0_out > 0)
                     )
                     ORDER BY se.timestamp DESC 
                     LIMIT 1), 0
                ) as current_price,
                -- 24小时成交量
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN se.amount0_in > 0 AND tp.token0 = $2 THEN se.amount0_in
                            WHEN se.amount1_in > 0 AND tp.token1 = $2 THEN se.amount1_in
                            WHEN se.amount0_out > 0 AND tp.token0 = $2 THEN se.amount0_out
                            WHEN se.amount1_out > 0 AND tp.token1 = $2 THEN se.amount1_out
                            ELSE 0
                        END
                    ) FROM swap_events se
                     JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
                     WHERE se.chain_id = $1 AND (tp.token0 = $2 OR tp.token1 = $2)
                     AND se.timestamp >= NOW() - INTERVAL '24 hours'), 0
                ) as volume_24h
        )
        SELECT 
            current_price,
            0 as price_change_1h,
            0 as price_change_24h,
            0 as price_change_7d,
            volume_24h,
            NULL as market_cap,
            NULL as fdv,
            NOW() as last_updated
        FROM token_prices
    "#;

    let row = sqlx::query(query)
        .bind(chain_id)
        .bind(address)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        Ok(Some(TokenPriceInfo {
            current_price: safe_get_decimal(&row, "current_price"),
            price_change_1h: safe_get_decimal(&row, "price_change_1h"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            price_change_7d: safe_get_decimal(&row, "price_change_7d"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            market_cap: safe_get_optional_decimal(&row, "market_cap"),
            fdv: safe_get_optional_decimal(&row, "fdv"),
            last_updated: safe_get_datetime(&row, "last_updated"),
        }))
    } else {
        Ok(None)
    }
}

async fn get_token_trading_pairs(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<Vec<TradingPairInfo>> {
    let query = r#"
        SELECT 
            tp.address as pair_address,
            CASE 
                WHEN tp.token0 = $2 THEN COALESCE(tp.token1_symbol, 'UNKNOWN')
                ELSE COALESCE(tp.token0_symbol, 'UNKNOWN')
            END as other_token_symbol,
            CASE 
                WHEN tp.token0 = $2 THEN COALESCE(tp.token1_name, 'Unknown Token')
                ELSE COALESCE(tp.token0_name, 'Unknown Token')
            END as other_token_name,
            -- 当前价格
            COALESCE(
                (SELECT 
                    CASE 
                        WHEN se.amount0_in > 0 AND se.amount1_out > 0 AND tp.token0 = $2 THEN se.amount0_in / se.amount1_out
                        WHEN se.amount1_in > 0 AND se.amount0_out > 0 AND tp.token1 = $2 THEN se.amount0_out / se.amount1_in
                        WHEN se.amount0_in > 0 AND se.amount1_out > 0 AND tp.token1 = $2 THEN se.amount1_out / se.amount0_in
                        WHEN se.amount1_in > 0 AND se.amount0_out > 0 AND tp.token0 = $2 THEN se.amount1_in / se.amount0_out
                        ELSE 0
                    END
                 FROM swap_events se
                 WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                 AND (
                     (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                     (se.amount1_in > 0 AND se.amount0_out > 0)
                 )
                 ORDER BY se.timestamp DESC 
                 LIMIT 1), 0
            ) as price,
            -- 24小时成交量
            COALESCE(
                (SELECT SUM(
                    CASE 
                        WHEN se.amount0_in > 0 AND tp.token0 = $2 THEN se.amount0_in
                        WHEN se.amount1_in > 0 AND tp.token1 = $2 THEN se.amount1_in
                        WHEN se.amount0_out > 0 AND tp.token0 = $2 THEN se.amount0_out
                        WHEN se.amount1_out > 0 AND tp.token1 = $2 THEN se.amount1_out
                        ELSE 0
                    END
                ) FROM swap_events se
                 WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id
                 AND se.timestamp >= NOW() - INTERVAL '24 hours'), 0
            ) as volume_24h,
            0 as liquidity
        FROM trading_pairs tp
        WHERE tp.chain_id = $1 AND (tp.token0 = $2 OR tp.token1 = $2)
        ORDER BY volume_24h DESC
    "#;

    let rows = sqlx::query(query)
        .bind(chain_id)
        .bind(address)
        .fetch_all(pool)
        .await?;

    let mut pairs = Vec::new();
    for row in rows {
        pairs.push(TradingPairInfo {
            pair_address: safe_get_string(&row, "pair_address"),
            other_token_symbol: safe_get_string(&row, "other_token_symbol"),
            other_token_name: safe_get_string(&row, "other_token_name"),
            price: safe_get_decimal(&row, "price"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            liquidity: safe_get_decimal(&row, "liquidity"),
        });
    }

    Ok(pairs)
}

pub async fn search_tokens(
    pool: &PgPool,
    query: &str,
    chain_id: Option<i32>,
    limit: i32,
) -> Result<Vec<TokenListItem>> {
    let search_term = format!("%{}%", query.to_lowercase());
    
    let chain_filter = if let Some(chain_id) = chain_id {
        format!("AND tm.chain_id = {}", chain_id)
    } else {
        "".to_string()
    };

    let sql_query = format!(
        r#"
        SELECT 
            1 as rank,
            tm.chain_id,
            CASE 
                WHEN tm.chain_id = 1 THEN 'Ethereum'
                WHEN tm.chain_id = 56 THEN 'BSC'
                WHEN tm.chain_id = 137 THEN 'Polygon'
                WHEN tm.chain_id = 42161 THEN 'Arbitrum'
                ELSE 'Unknown'
            END as chain_name,
            '' as pair_address,
            tm.symbol as token0_symbol,
            '' as token1_symbol,
            tm.name as token0_name,
            '' as token1_name,
            tm.logo_url as token0_logo_url,
            NULL as token1_logo_url,
            tm.website_url as token0_website_url,
            NULL as token1_website_url,
            tm.explorer_url as token0_explorer_url,
            NULL as token1_explorer_url,
            tm.description as token0_description,
            NULL as token1_description,
            tm.tags as token0_tags,
            NULL as token1_tags,
            0 as price_usd,
            0 as price_change_1h,
            0 as price_change_24h,
            0 as volume_1h,
            0 as volume_24h,
            NULL as fdv,
            NULL as market_cap,
            0 as liquidity,
            tm.updated_at as last_updated
        FROM token_metadata tm
        WHERE (
            LOWER(tm.symbol) LIKE $1 OR 
            LOWER(tm.name) LIKE $1 OR 
            LOWER(tm.address) LIKE $1
        )
        {}
        ORDER BY 
            CASE WHEN LOWER(tm.symbol) = LOWER($2) THEN 1 ELSE 2 END,
            CASE WHEN LOWER(tm.name) = LOWER($2) THEN 1 ELSE 2 END,
            tm.symbol
        LIMIT $3
        "#,
        chain_filter
    );

    let rows = sqlx::query(&sql_query)
        .bind(&search_term)
        .bind(query)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut tokens = Vec::new();
    for row in rows {
        let token0_tags: Option<Vec<String>> = safe_get_optional_string(&row, "token0_tags")
            .and_then(|s| serde_json::from_str(&s).ok());

        tokens.push(TokenListItem {
            rank: safe_get_i32(&row, "rank"),
            chain_id: safe_get_i32(&row, "chain_id"),
            chain_name: safe_get_string(&row, "chain_name"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_string(&row, "token0_symbol"),
            token1_symbol: safe_get_string(&row, "token1_symbol"),
            token0_name: safe_get_string(&row, "token0_name"),
            token1_name: safe_get_string(&row, "token1_name"),
            token0_logo_url: safe_get_optional_string(&row, "token0_logo_url"),
            token1_logo_url: safe_get_optional_string(&row, "token1_logo_url"),
            token0_website_url: safe_get_optional_string(&row, "token0_website_url"),
            token1_website_url: safe_get_optional_string(&row, "token1_website_url"),
            token0_explorer_url: safe_get_optional_string(&row, "token0_explorer_url"),
            token1_explorer_url: safe_get_optional_string(&row, "token1_explorer_url"),
            token0_description: safe_get_optional_string(&row, "token0_description"),
            token1_description: safe_get_optional_string(&row, "token1_description"),
            token0_tags,
            token1_tags: None,
            price_usd: safe_get_decimal(&row, "price_usd"),
            price_change_1h: safe_get_decimal(&row, "price_change_1h"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            volume_1h: safe_get_decimal(&row, "volume_1h"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            fdv: safe_get_optional_decimal(&row, "fdv"),
            market_cap: safe_get_optional_decimal(&row, "market_cap"),
            liquidity: safe_get_decimal(&row, "liquidity"),
            last_updated: safe_get_datetime(&row, "last_updated"),
        });
    }

    Ok(tokens)
}

pub async fn get_trending_tokens(
    pool: &PgPool,
    chain_id: Option<i32>,
    limit: i32,
) -> Result<Vec<TokenListItem>> {
    // 基于24小时价格变化和成交量的趋势算法
    get_token_list(pool, chain_id, limit, 0, "volume", "desc").await
}

pub async fn get_new_tokens(
    pool: &PgPool,
    chain_id: Option<i32>,
    limit: i32,
) -> Result<Vec<TokenListItem>> {
    let chain_filter = if let Some(chain_id) = chain_id {
        format!("WHERE tp.chain_id = {}", chain_id)
    } else {
        "".to_string()
    };

    let query = format!(
        r#"
        SELECT 
            ROW_NUMBER() OVER (ORDER BY tp.created_at DESC) as rank,
            tp.chain_id,
            CASE 
                WHEN tp.chain_id = 1 THEN 'Ethereum'
                WHEN tp.chain_id = 56 THEN 'BSC'
                WHEN tp.chain_id = 137 THEN 'Polygon'
                WHEN tp.chain_id = 42161 THEN 'Arbitrum'
                ELSE 'Unknown'
            END as chain_name,
            tp.address as pair_address,
            COALESCE(tp.token0_symbol, 'UNKNOWN') as token0_symbol,
            COALESCE(tp.token1_symbol, 'UNKNOWN') as token1_symbol,
            COALESCE(tp.token0_name, 'Unknown Token') as token0_name,
            COALESCE(tp.token1_name, 'Unknown Token') as token1_name,
            tm0.logo_url as token0_logo_url,
            tm1.logo_url as token1_logo_url,
            tm0.website_url as token0_website_url,
            tm1.website_url as token1_website_url,
            tm0.explorer_url as token0_explorer_url,
            tm1.explorer_url as token1_explorer_url,
            tm0.description as token0_description,
            tm1.description as token1_description,
            tm0.tags as token0_tags,
            tm1.tags as token1_tags,
            0 as price_usd,
            0 as price_change_1h,
            0 as price_change_24h,
            0 as volume_1h,
            0 as volume_24h,
            NULL as fdv,
            NULL as market_cap,
            0 as liquidity,
            tp.created_at as last_updated
        FROM trading_pairs tp
        LEFT JOIN token_metadata tm0 ON tm0.chain_id = tp.chain_id AND tm0.address = tp.token0
        LEFT JOIN token_metadata tm1 ON tm1.chain_id = tp.chain_id AND tm1.address = tp.token1
        {}
        ORDER BY tp.created_at DESC
        LIMIT {}
        "#,
        chain_filter, limit
    );

    let rows = sqlx::query(&query).fetch_all(pool).await?;

    let mut tokens = Vec::new();
    for row in rows {
        let token0_tags: Option<Vec<String>> = safe_get_optional_string(&row, "token0_tags")
            .and_then(|s| serde_json::from_str(&s).ok());
        let token1_tags: Option<Vec<String>> = safe_get_optional_string(&row, "token1_tags")
            .and_then(|s| serde_json::from_str(&s).ok());

        tokens.push(TokenListItem {
            rank: safe_get_i32(&row, "rank"),
            chain_id: safe_get_i32(&row, "chain_id"),
            chain_name: safe_get_string(&row, "chain_name"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_string(&row, "token0_symbol"),
            token1_symbol: safe_get_string(&row, "token1_symbol"),
            token0_name: safe_get_string(&row, "token0_name"),
            token1_name: safe_get_string(&row, "token1_name"),
            token0_logo_url: safe_get_optional_string(&row, "token0_logo_url"),
            token1_logo_url: safe_get_optional_string(&row, "token1_logo_url"),
            token0_website_url: safe_get_optional_string(&row, "token0_website_url"),
            token1_website_url: safe_get_optional_string(&row, "token1_website_url"),
            token0_explorer_url: safe_get_optional_string(&row, "token0_explorer_url"),
            token1_explorer_url: safe_get_optional_string(&row, "token1_explorer_url"),
            token0_description: safe_get_optional_string(&row, "token0_description"),
            token1_description: safe_get_optional_string(&row, "token1_description"),
            token0_tags,
            token1_tags,
            price_usd: safe_get_decimal(&row, "price_usd"),
            price_change_1h: safe_get_decimal(&row, "price_change_1h"),
            price_change_24h: safe_get_decimal(&row, "price_change_24h"),
            volume_1h: safe_get_decimal(&row, "volume_1h"),
            volume_24h: safe_get_decimal(&row, "volume_24h"),
            fdv: safe_get_optional_decimal(&row, "fdv"),
            market_cap: safe_get_optional_decimal(&row, "market_cap"),
            liquidity: safe_get_decimal(&row, "liquidity"),
            last_updated: safe_get_datetime(&row, "last_updated"),
        });
    }

    Ok(tokens)
}

// Wallet相关操作
pub async fn get_wallet_transactions(
    pool: &PgPool,
    wallet_address: &str,
    chain_id: Option<i32>,
    limit: i32,
    offset: i32,
    transaction_type: Option<&str>,
) -> Result<Vec<WalletTransaction>> {
    let mut conditions = vec!["(se.sender = $1 OR se.to_address = $1)".to_string()];
    let mut param_count = 1;

    if let Some(chain_id) = chain_id {
        param_count += 1;
        conditions.push(format!("se.chain_id = ${}", param_count));
    }

    if let Some(tx_type) = transaction_type {
        match tx_type {
            "swap" => {
                // 只查询swap事件
            }
            "mint" | "burn" => {
                // 这里需要联合查询mint和burn事件，暂时先返回空
                return Ok(vec![]);
            }
            _ => {}
        }
    }

    let where_clause = conditions.join(" AND ");
    
    let query = format!(
        r#"
        SELECT 
            se.id,
            se.chain_id,
            se.pair_address,
            tp.token0_symbol,
            tp.token1_symbol,
            se.transaction_hash,
            $1 as wallet_address,
            'swap' as transaction_type,
            se.amount0_in + se.amount0_out as amount0,
            se.amount1_in + se.amount1_out as amount1,
            CASE 
                WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN se.amount0_in / se.amount1_out
                WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN se.amount0_out / se.amount1_in
                ELSE 0
            END as price,
            se.block_number,
            se.timestamp
        FROM swap_events se
        LEFT JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
        WHERE {}
        ORDER BY se.timestamp DESC
        LIMIT ${} OFFSET ${}
        "#,
        where_clause, param_count + 1, param_count + 2
    );

    let mut query_builder = sqlx::query(&query).bind(wallet_address);
    
    if let Some(chain_id) = chain_id {
        query_builder = query_builder.bind(chain_id);
    }
    
    let rows = query_builder
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let mut transactions = Vec::new();
    for row in rows {
        transactions.push(WalletTransaction {
            id: safe_get_uuid(&row, "id"),
            chain_id: safe_get_i32(&row, "chain_id"),
            pair_address: safe_get_string(&row, "pair_address"),
            token0_symbol: safe_get_optional_string(&row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(&row, "token1_symbol"),
            transaction_hash: safe_get_string(&row, "transaction_hash"),
            wallet_address: safe_get_string(&row, "wallet_address"),
            transaction_type: safe_get_string(&row, "transaction_type"),
            amount0: safe_get_decimal(&row, "amount0"),
            amount1: safe_get_decimal(&row, "amount1"),
            price: Some(safe_get_decimal(&row, "price")),
            value_usd: None,
            block_number: safe_get_i64(&row, "block_number"),
            timestamp: safe_get_datetime(&row, "timestamp"),
        });
    }

    Ok(transactions)
}

pub async fn get_wallet_stats(
    pool: &PgPool,
    wallet_address: &str,
    chain_id: Option<i32>,
    days: i32,
) -> Result<Option<WalletStats>> {
    let chain_filter = if let Some(chain_id) = chain_id {
        format!("AND se.chain_id = {}", chain_id)
    } else {
        "".to_string()
    };

    let query = format!(
        r#"
        WITH wallet_activity AS (
            SELECT 
                COUNT(*) as total_transactions,
                SUM(
                    CASE 
                        WHEN se.amount0_in > 0 THEN se.amount0_in
                        WHEN se.amount1_in > 0 THEN se.amount1_in  
                        ELSE 0
                    END
                ) as total_volume_usd,
                MIN(se.timestamp) as first_transaction,
                MAX(se.timestamp) as last_transaction
            FROM swap_events se
            WHERE (se.sender = $1 OR se.to_address = $1)
            AND se.timestamp >= NOW() - INTERVAL '1 day' * $2
            {}
        )
        SELECT 
            $1 as wallet_address,
            {} as chain_id,
            COALESCE(total_transactions, 0) as total_transactions,
            COALESCE(total_volume_usd, 0) as total_volume_usd,
            0 as total_fees_paid,
            0 as profit_loss,
            0 as win_rate,
            CASE 
                WHEN total_transactions > 0 THEN total_volume_usd / total_transactions
                ELSE 0
            END as avg_trade_size,
            first_transaction,
            last_transaction
        FROM wallet_activity
        "#,
        chain_filter,
        chain_id.map(|c| c.to_string()).unwrap_or_else(|| "NULL".to_string())
    );

    let row = sqlx::query(&query)
        .bind(wallet_address)
        .bind(days)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        Ok(Some(WalletStats {
            wallet_address: safe_get_string(&row, "wallet_address"),
            chain_id: safe_get_optional_i32(&row, "chain_id"),
            total_transactions: safe_get_i64(&row, "total_transactions"),
            total_volume_usd: safe_get_decimal(&row, "total_volume_usd"),
            total_fees_paid: safe_get_decimal(&row, "total_fees_paid"),
            profit_loss: safe_get_decimal(&row, "profit_loss"),
            win_rate: safe_get_decimal(&row, "win_rate"),
            avg_trade_size: safe_get_decimal(&row, "avg_trade_size"),
            first_transaction: safe_get_optional_datetime(&row, "first_transaction").unwrap_or_else(|| Utc::now()),
            last_transaction: safe_get_optional_datetime(&row, "last_transaction").unwrap_or_else(|| Utc::now()),
        }))
    } else {
        Ok(None)
    }
}

pub async fn get_wallet_portfolio(
    pool: &PgPool,
    wallet_address: &str,
    chain_id: Option<i32>,
) -> Result<Vec<WalletPortfolioItem>> {
    // 这是一个复杂的查询，需要计算钱包的代币余额
    // 暂时返回空数组，实际实现需要���踪所有的mint/burn/swap事件来计算余额
    Ok(vec![])
}

pub async fn get_wallet_pnl(
    pool: &PgPool,
    wallet_address: &str,
    chain_id: Option<i32>,
    days: i32,
) -> Result<Vec<WalletPnLRecord>> {
    // 这是一个复杂的查询，需要计算每日的盈亏
    // 暂时返回空数组，实际实现需要复杂的盈亏计算逻辑
    Ok(vec![])
}

// Chain stats相关操作
pub async fn get_chain_stats(
    pool: &PgPool,
    chain_id: Option<i32>,
) -> Result<Vec<ChainStats>> {
    let chain_filter = if let Some(chain_id) = chain_id {
        format!("WHERE tp.chain_id = {}", chain_id)
    } else {
        "".to_string()
    };

    let query = format!(
        r#"
        WITH chain_activity AS (
            SELECT 
                tp.chain_id,
                COUNT(DISTINCT tp.address) as total_pairs,
                COALESCE(SUM(
                    CASE 
                        WHEN se.amount0_in > 0 THEN se.amount0_in
                        WHEN se.amount1_in > 0 THEN se.amount1_in  
                        ELSE 0
                    END
                ), 0) as total_volume_24h,
                COUNT(DISTINCT CASE 
                    WHEN se.timestamp >= NOW() - INTERVAL '24 hours' THEN tp.address 
                END) as active_pairs_24h
            FROM trading_pairs tp
            LEFT JOIN swap_events se ON se.pair_address = tp.address 
                AND se.chain_id = tp.chain_id 
                AND se.timestamp >= NOW() - INTERVAL '24 hours'
            {}
            GROUP BY tp.chain_id
        )
        SELECT 
            chain_id,
            CASE 
                WHEN chain_id = 1 THEN 'Ethereum'
                WHEN chain_id = 56 THEN 'BSC'
                WHEN chain_id = 137 THEN 'Polygon'
                WHEN chain_id = 42161 THEN 'Arbitrum'
                ELSE 'Unknown'
            END as chain_name,
            total_pairs,
            total_volume_24h,
            0 as total_liquidity,
            active_pairs_24h
        FROM chain_activity
        ORDER BY chain_id
        "#,
        chain_filter
    );

    let rows = sqlx::query(&query).fetch_all(pool).await?;

    let mut stats = Vec::new();
    for row in rows {
        stats.push(ChainStats {
            chain_id: safe_get_i32(&row, "chain_id"),
            chain_name: safe_get_string(&row, "chain_name"),
            total_pairs: safe_get_i64(&row, "total_pairs"),
            total_volume_24h: safe_get_decimal(&row, "total_volume_24h"),
            total_liquidity: safe_get_decimal(&row, "total_liquidity"),
            active_pairs_24h: safe_get_i64(&row, "active_pairs_24h"),
        });
    }

    Ok(stats)
}

pub async fn get_system_health(pool: &PgPool) -> Result<SystemHealth> {
    // 获取最新处理的区块
    let latest_block_query = r#"
        SELECT MAX(last_block_number) as latest_block
        FROM last_processed_blocks
    "#;
    
    let latest_block: i64 = sqlx::query_scalar(latest_block_query)
        .fetch_optional(pool)
        .await?
        .unwrap_or(0);

    // 获取事件监听器状态
    let listeners_query = r#"
        SELECT 
            chain_id,
            event_type,
            last_block_number,
            updated_at,
            CASE 
                WHEN updated_at >= NOW() - INTERVAL '5 minutes' THEN 'healthy'
                WHEN updated_at >= NOW() - INTERVAL '15 minutes' THEN 'warning'
                ELSE 'error'
            END as status
        FROM last_processed_blocks
        ORDER BY chain_id, event_type
    "#;

    let listener_rows = sqlx::query(listeners_query).fetch_all(pool).await?;
    
    let mut event_listeners = Vec::new();
    for row in listener_rows {
        event_listeners.push(EventListenerStatus {
            chain_id: safe_get_i32(&row, "chain_id"),
            event_type: safe_get_string(&row, "event_type"),
            status: safe_get_string(&row, "status"),
            last_processed_block: safe_get_i64(&row, "last_block_number"),
            blocks_behind: 0, // 需要从外部获取当前区块高度来计算
            last_updated: safe_get_datetime(&row, "updated_at"),
        });
    }

    // 检查数据库连接
    let db_status = match sqlx::query("SELECT 1").fetch_optional(pool).await {
        Ok(_) => "healthy",
        Err(_) => "error",
    };

    // 计算系统整体状态
    let overall_status = if db_status == "healthy" && 
        event_listeners.iter().all(|l| l.status == "healthy") {
        "healthy"
    } else if event_listeners.iter().any(|l| l.status == "error") {
        "error"
    } else {
        "warning"
    };

    Ok(SystemHealth {
        status: overall_status.to_string(),
        database_status: db_status.to_string(),
        event_listeners_status: event_listeners,
        last_block_processed: latest_block,
        blocks_behind: 0, // 需要从外部获取当前区块高度来计算
        uptime_seconds: 0, // 需要从应用启动时间计算
    })
}

// Token metadata相关操作
pub async fn create_token_metadata(
    pool: &PgPool,
    metadata: &CreateTokenMetadata,
) -> Result<TokenMetadata> {
    let id = Uuid::new_v4();
    let now = Utc::now();

    let tags_json = metadata.tags.as_ref()
        .map(|tags| serde_json::to_value(tags))
        .transpose()?;

    sqlx::query(
        r#"
        INSERT INTO token_metadata 
        (id, chain_id, address, symbol, name, decimals, description, website_url, logo_url, 
         twitter_url, telegram_url, discord_url, github_url, explorer_url, coingecko_id, 
         coinmarketcap_id, total_supply, max_supply, is_verified, tags, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        "#,
    )
    .bind(&id)
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
    .bind(false) // is_verified defaults to false
    .bind(&tags_json)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(TokenMetadata {
        id,
        chain_id: metadata.chain_id,
        address: metadata.address.clone(),
        symbol: metadata.symbol.clone(),
        name: metadata.name.clone(),
        decimals: metadata.decimals,
        description: metadata.description.clone(),
        website_url: metadata.website_url.clone(),
        logo_url: metadata.logo_url.clone(),
        twitter_url: metadata.twitter_url.clone(),
        telegram_url: metadata.telegram_url.clone(),
        discord_url: metadata.discord_url.clone(),
        github_url: metadata.github_url.clone(),
        explorer_url: metadata.explorer_url.clone(),
        coingecko_id: metadata.coingecko_id.clone(),
        coinmarketcap_id: metadata.coinmarketcap_id.clone(),
        total_supply: metadata.total_supply,
        max_supply: metadata.max_supply,
        is_verified: false,
        tags: metadata.tags.clone(),
        created_at: now,
        updated_at: now,
    })
}

pub async fn get_token_metadata(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<Option<TokenMetadata>> {
    let row = sqlx::query(
        "SELECT * FROM token_metadata WHERE chain_id = $1 AND address = $2"
    )
    .bind(chain_id)
    .bind(address)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = row {
        let tags: Option<Vec<String>> = safe_get_optional_string(&row, "tags")
            .and_then(|s| serde_json::from_str(&s).ok());

        Ok(Some(TokenMetadata {
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
        }))
    } else {
        Ok(None)
    }
}

pub async fn update_token_metadata(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
    update: &UpdateTokenMetadata,
) -> Result<Option<TokenMetadata>> {
    let now = Utc::now();
    let tags_json = update.tags.as_ref()
        .map(|tags| serde_json::to_value(tags))
        .transpose()?;

    let result = sqlx::query(
        r#"
        UPDATE token_metadata 
        SET symbol = COALESCE($3, symbol),
            name = COALESCE($4, name),
            decimals = COALESCE($5, decimals),
            description = COALESCE($6, description),
            website_url = COALESCE($7, website_url),
            logo_url = COALESCE($8, logo_url),
            twitter_url = COALESCE($9, twitter_url),
            telegram_url = COALESCE($10, telegram_url),
            discord_url = COALESCE($11, discord_url),
            github_url = COALESCE($12, github_url),
            explorer_url = COALESCE($13, explorer_url),
            coingecko_id = COALESCE($14, coingecko_id),
            coinmarketcap_id = COALESCE($15, coinmarketcap_id),
            total_supply = COALESCE($16, total_supply),
            max_supply = COALESCE($17, max_supply),
            tags = COALESCE($18, tags),
            updated_at = $19
        WHERE chain_id = $1 AND address = $2
        "#,
    )
    .bind(chain_id)
    .bind(address)
    .bind(&update.symbol)
    .bind(&update.name)
    .bind(&update.decimals)
    .bind(&update.description)
    .bind(&update.website_url)
    .bind(&update.logo_url)
    .bind(&update.twitter_url)
    .bind(&update.telegram_url)
    .bind(&update.discord_url)
    .bind(&update.github_url)
    .bind(&update.explorer_url)
    .bind(&update.coingecko_id)
    .bind(&update.coinmarketcap_id)
    .bind(&update.total_supply)
    .bind(&update.max_supply)
    .bind(&tags_json)
    .bind(&now)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        get_token_metadata(pool, chain_id, address).await
    } else {
        Ok(None)
    }
}

pub async fn delete_token_metadata(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "DELETE FROM token_metadata WHERE chain_id = $1 AND address = $2"
    )
    .bind(chain_id)
    .bind(address)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn list_token_metadata(
    pool: &PgPool,
    chain_id: Option<i32>,
    limit: i32,
    offset: i32,
    verified_only: bool,
) -> Result<Vec<TokenMetadata>> {
    let mut conditions = Vec::new();
    let mut param_count = 0;

    if let Some(chain_id) = chain_id {
        param_count += 1;
        conditions.push(format!("chain_id = ${}", param_count));
    }

    if verified_only {
        conditions.push("is_verified = true".to_string());
    }

    let where_clause = if conditions.is_empty() {
        "".to_string()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query = format!(
        r#"
        SELECT * FROM token_metadata
        {}
        ORDER BY updated_at DESC
        LIMIT ${} OFFSET ${}
        "#,
        where_clause, param_count + 1, param_count + 2
    );

    let mut query_builder = sqlx::query(&query);
    
    if let Some(chain_id) = chain_id {
        query_builder = query_builder.bind(chain_id);
    }
    
    let rows = query_builder
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let mut metadata_list = Vec::new();
    for row in rows {
        let tags: Option<Vec<String>> = safe_get_optional_string(&row, "tags")
            .and_then(|s| serde_json::from_str(&s).ok());

        metadata_list.push(TokenMetadata {
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
        });
    }

    Ok(metadata_list)
}

pub async fn verify_token_metadata(
    pool: &PgPool,
    chain_id: i32,
    address: &str,
) -> Result<Option<TokenMetadata>> {
    let now = Utc::now();
    
    let result = sqlx::query(
        r#"
        UPDATE token_metadata 
        SET is_verified = true, updated_at = $3
        WHERE chain_id = $1 AND address = $2
        "#,
    )
    .bind(chain_id)
    .bind(address)
    .bind(&now)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        get_token_metadata(pool, chain_id, address).await
    } else {
        Ok(None)
    }
}
