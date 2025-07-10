use crate::types::*;
use crate::database::utils::*;
use anyhow::Result;
use sqlx::PgPool;

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
