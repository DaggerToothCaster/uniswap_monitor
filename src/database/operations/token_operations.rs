use crate::types::*;
use crate::database::utils::*;
use anyhow::Result;
use sqlx::PgPool;
use chrono::Utc;

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
            LEFT JOIN token_metadata tm0 ON tm0.chain_id = tp.chain_id AND tm0.address = tp.token0
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

pub async fn get_token_price_info(
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

pub async fn get_token_trading_pairs(
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

// 获取token元数据
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
