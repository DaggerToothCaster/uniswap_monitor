use crate::database::utils::*;
use crate::types::{
    KLineData, LiquidityRecord, PairStats, TimeSeriesData, TradeRecord, TradingPair,
    TradingPairWithStats,
};
use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::query_as;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use tracing::info;

// 时间间隔配置结构
#[derive(Debug)]
struct IntervalConfig {
    time_bucket_expr: &'static str,
    time_range: &'static str,
    default_limit: i32,
}

impl IntervalConfig {
    fn get_config() -> HashMap<&'static str, IntervalConfig> {
        let mut config = HashMap::new();

        config.insert(
            "1m",
            IntervalConfig {
                time_bucket_expr: "date_trunc('minute', timestamp)",
                time_range: "1 day",
                default_limit: 1440,
            },
        );

        config.insert("5m", IntervalConfig {
            time_bucket_expr: "date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 5)",
            time_range: "3 days",
            default_limit: 864,
        });

        config.insert("15m", IntervalConfig {
            time_bucket_expr: "date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 15)",
            time_range: "7 days",
            default_limit: 672,
        });

        config.insert("30m", IntervalConfig {
            time_bucket_expr: "date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 30)",
            time_range: "14 days",
            default_limit: 672,
        });

        config.insert(
            "1h",
            IntervalConfig {
                time_bucket_expr: "date_trunc('hour', timestamp)",
                time_range: "30 days",
                default_limit: 720,
            },
        );

        config.insert("4h", IntervalConfig {
            time_bucket_expr: "date_trunc('hour', timestamp) - INTERVAL '1 hour' * (EXTRACT(hour FROM timestamp)::int % 4)",
            time_range: "90 days",
            default_limit: 540,
        });

        config.insert(
            "1d",
            IntervalConfig {
                time_bucket_expr: "date_trunc('day', timestamp)",
                time_range: "1 year",
                default_limit: 365,
            },
        );

        config.insert(
            "1w",
            IntervalConfig {
                time_bucket_expr: "date_trunc('week', timestamp)",
                time_range: "2 years",
                default_limit: 104,
            },
        );

        config.insert(
            "1M",
            IntervalConfig {
                time_bucket_expr: "date_trunc('month', timestamp)",
                time_range: "5 years",
                default_limit: 60,
            },
        );

        config.insert(
            "1y",
            IntervalConfig {
                time_bucket_expr: "date_trunc('year', timestamp)",
                time_range: "10 years",
                default_limit: 10,
            },
        );

        config
    }
}

pub struct TradingOperations;

impl TradingOperations {
    /// 插入交易对（由事件服务触发）
    ///
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

    /// 获取交易对列表（带分页信息）
    ///
    /// # 参数
    /// * `pool` - 数据库连接池
    /// * `chain_id` - 链ID筛选条件：
    ///   - `None`: 查询所有链的交易对
    ///   - `Some(0)`: 查询所有链的交易对（与None等效）
    ///   - `Some(n)`: 只查询指定链ID的交易对
    /// * `limit` - 每页记录数：
    ///   - `None`: 返回所有记录（无分页）
    ///   - `Some(n)`: 返回最多n条记录
    /// * `offset` - 分页偏移量：
    ///   - `None`: 从第一条记录开始
    ///   - `Some(n)`: 跳过前n条记录
    ///
    /// # 返回值
    /// 返回 `Result<(Vec<TradingPairWithStats>, i64)>` 元组：
    /// - `Vec<TradingPairWithStats>`: 查询到的交易对列表
    /// - `i64`: 符合条件的总记录数（不考虑分页）
    ///
    /// # 示例
    /// ```rust
    /// // 查询以太坊(chain_id=1)的前10条交易对
    /// let (pairs, total) = get_all_pairs(&pool, Some(1), Some(10), Some(0)).await?;
    /// // 查询所有交易对（不分页）
    /// let (all_pairs, total) = get_all_pairs(&pool, None, None, None)).await?;
    ///
    pub async fn get_all_pairs(
        pool: &PgPool,
        chain_id: Option<i32>,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TradingPairWithStats>, i64), sqlx::Error> {
        let has_chain_filter = chain_id.unwrap_or(0) != 0;
        let base_query = format!(
            r#"
    WITH latest_swap AS (
        SELECT 
            se.pair_address,
            se.chain_id,
            MAX(se.timestamp) as latest_timestamp
        FROM swap_events se
        {}
        GROUP BY se.pair_address, se.chain_id
    ),
    price_data AS (
        SELECT
            se.pair_address,
            se.chain_id,
            MAX(CASE WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN 
                ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) / 
                NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0) 
            END)::NUMERIC as current_price,
            MAX(CASE WHEN se.timestamp <= NOW() - INTERVAL '24 hours' AND se.amount0_in > 0 AND se.amount1_out > 0 THEN 
                ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) / 
                NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0) 
            END)::NUMERIC as price_24h_ago
        FROM swap_events se
        JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
        {}
        GROUP BY se.pair_address, se.chain_id
    ),
    pair_stats AS (
        SELECT 
            tp.id,
            tp.address AS pair_address,
            tp.chain_id,
            tp.token0,
            tp.token1,
            tp.token0_symbol,
            tp.token1_symbol,
            tp.token0_decimals,
            tp.token1_decimals,
            tp.created_at,
            ls.latest_timestamp AS last_updated,
            COALESCE(pd.current_price, 0)::NUMERIC(38,18) AS price,
            COALESCE(1 / NULLIF(pd.current_price, 0), 0)::NUMERIC(38,18) AS inverted_price,
            COALESCE(
                ((pd.current_price - pd.price_24h_ago) / NULLIF(pd.price_24h_ago, 0)) * 100, 
                0
            )::NUMERIC(38,18) AS price_24h_change,

            -- 24h volumes
            (
                SELECT COALESCE(SUM(
                    ((se.amount0_in + se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC)
                )::NUMERIC, 0)
                FROM swap_events se 
                WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                AND se.timestamp >= NOW() - INTERVAL '24 hours'
            ) AS volume_24h_token0,
            (
                SELECT COALESCE(SUM(
                    ((se.amount1_in + se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC)
                )::NUMERIC, 0)
                FROM swap_events se 
                WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                AND se.timestamp >= NOW() - INTERVAL '24 hours'
            ) AS volume_24h_token1,

            -- 24h tx count
            (
                SELECT COUNT(*) FROM swap_events se 
                WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                AND se.timestamp >= NOW() - INTERVAL '24 hours'
            ) AS tx_count_24h,

            -- token0 liquidity
            (
                SELECT COALESCE(SUM(
                    (me.amount0)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                )::NUMERIC, 0)
                FROM mint_events me 
                WHERE me.pair_address = tp.address AND me.chain_id = tp.chain_id
            ) -
            (
                SELECT COALESCE(SUM(
                    (be.amount0)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                )::NUMERIC, 0)
                FROM burn_events be 
                WHERE be.pair_address = tp.address AND be.chain_id = tp.chain_id
            ) AS liquidity_token0,

            -- token1 liquidity
            (
                SELECT COALESCE(SUM(
                    (me.amount1)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                )::NUMERIC, 0)
                FROM mint_events me 
                WHERE me.pair_address = tp.address AND me.chain_id = tp.chain_id
            ) -
            (
                SELECT COALESCE(SUM(
                    (be.amount1)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                )::NUMERIC, 0)
                FROM burn_events be 
                WHERE be.pair_address = tp.address AND be.chain_id = tp.chain_id
            ) AS liquidity_token1
        FROM trading_pairs tp
        LEFT JOIN latest_swap ls ON tp.address = ls.pair_address AND tp.chain_id = ls.chain_id
        LEFT JOIN price_data pd ON tp.address = pd.pair_address AND tp.chain_id = pd.chain_id
        {}
    )
    SELECT * FROM pair_stats
    ORDER BY volume_24h_token0 DESC
    LIMIT ${} OFFSET ${}
    "#,
            if has_chain_filter {
                "WHERE se.chain_id = $1"
            } else {
                ""
            },
            if has_chain_filter {
                "WHERE se.chain_id = $1"
            } else {
                ""
            },
            if has_chain_filter {
                "WHERE tp.chain_id = $1"
            } else {
                ""
            },
            if has_chain_filter { 2 } else { 1 },
            if has_chain_filter { 3 } else { 2 }
        );

        let pairs = if has_chain_filter {
            sqlx::query_as::<_, TradingPairWithStats>(&base_query)
                .bind(chain_id.unwrap())
                .bind(limit.unwrap_or(i32::MAX))
                .bind(offset.unwrap_or(0))
                .fetch_all(pool)
                .await?
        } else {
            sqlx::query_as::<_, TradingPairWithStats>(&base_query)
                .bind(limit.unwrap_or(i32::MAX))
                .bind(offset.unwrap_or(0))
                .fetch_all(pool)
                .await?
        };

        let count_query = if has_chain_filter {
            "SELECT COUNT(*) FROM trading_pairs WHERE chain_id = $1"
        } else {
            "SELECT COUNT(*) FROM trading_pairs"
        };
        let total = if has_chain_filter {
            sqlx::query_scalar(count_query)
                .bind(chain_id.unwrap())
                .fetch_one(pool)
                .await?
        } else {
            sqlx::query_scalar(count_query).fetch_one(pool).await?
        };

        Ok((pairs, total))
    }

    pub async fn get_db_pairs(
        pool: &PgPool,
        chain_id: Option<i32>,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<Vec<TradingPair>> {
        let query = if let Some(chain_id) = chain_id {
            sqlx::query_as::<_, TradingPair>(
                "SELECT * FROM trading_pairs 
             WHERE chain_id = $1 
             ORDER BY created_at DESC
             LIMIT $2 OFFSET $3",
            )
            .bind(chain_id)
            .bind(limit)
            .bind(offset)
        } else {
            sqlx::query_as::<_, TradingPair>(
                "SELECT * FROM trading_pairs 
             ORDER BY created_at DESC
             LIMIT $1 OFFSET $2",
            )
            .bind(limit)
            .bind(offset)
        };

        let pairs = query.fetch_all(pool).await?;
        Ok(pairs)
    }

    /// 获取某个交易对的详情（包含统计信息）
    pub async fn get_pair_detail(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Option<TradingPairWithStats>, sqlx::Error> {
        let query = r#"
        WITH latest_swap AS (
            SELECT 
                se.pair_address,
                se.chain_id,
                MAX(se.timestamp) as latest_timestamp
            FROM swap_events se
            WHERE se.pair_address = $1 AND se.chain_id = $2
            GROUP BY se.pair_address, se.chain_id
        ),
        price_data AS (
            SELECT
                se.pair_address,
                se.chain_id,
                MAX(CASE WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN 
                    ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) / 
                    NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0) 
                END)::NUMERIC as current_price,
                MAX(CASE WHEN se.timestamp <= NOW() - INTERVAL '24 hours' AND se.amount0_in > 0 AND se.amount1_out > 0 THEN 
                    ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) / 
                    NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0) 
                END)::NUMERIC as price_24h_ago
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            GROUP BY se.pair_address, se.chain_id
        ),
        pair_stats AS (
            SELECT 
                tp.id,
                tp.address AS pair_address,
                tp.chain_id,
                tp.token0,
                tp.token1,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                tp.created_at,
                ls.latest_timestamp AS last_updated,
                COALESCE(pd.current_price, 0)::NUMERIC(38,18) AS price,
                COALESCE(1 / NULLIF(pd.current_price, 0), 0)::NUMERIC(38,18) AS inverted_price,
                COALESCE(
                    ((pd.current_price - pd.price_24h_ago) / NULLIF(pd.price_24h_ago, 0)) * 100, 
                    0
                )::NUMERIC(38,18) AS price_24h_change,

                -- 24h volumes
                (
                    SELECT COALESCE(SUM(
                        ((se.amount0_in + se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC)
                    )::NUMERIC, 0)
                    FROM swap_events se 
                    WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                    AND se.timestamp >= NOW() - INTERVAL '24 hours'
                ) AS volume_24h_token0,

                (
                    SELECT COALESCE(SUM(
                        ((se.amount1_in + se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC)
                    )::NUMERIC, 0)
                    FROM swap_events se 
                    WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                    AND se.timestamp >= NOW() - INTERVAL '24 hours'
                ) AS volume_24h_token1,

                -- 24h tx count
                (
                    SELECT COUNT(*) FROM swap_events se 
                    WHERE se.pair_address = tp.address AND se.chain_id = tp.chain_id 
                    AND se.timestamp >= NOW() - INTERVAL '24 hours'
                ) AS tx_count_24h,

                -- token0 liquidity
                (
                    SELECT COALESCE(SUM(
                        (me.amount0)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                    )::NUMERIC, 0)
                    FROM mint_events me 
                    WHERE me.pair_address = tp.address AND me.chain_id = tp.chain_id
                ) -
                (
                    SELECT COALESCE(SUM(
                        (be.amount0)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                    )::NUMERIC, 0)
                    FROM burn_events be 
                    WHERE be.pair_address = tp.address AND be.chain_id = tp.chain_id
                ) AS liquidity_token0,

                -- token1 liquidity
                (
                    SELECT COALESCE(SUM(
                        (me.amount1)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                    )::NUMERIC, 0)
                    FROM mint_events me 
                    WHERE me.pair_address = tp.address AND me.chain_id = tp.chain_id
                ) -
                (
                    SELECT COALESCE(SUM(
                        (be.amount1)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                    )::NUMERIC, 0)
                    FROM burn_events be 
                    WHERE be.pair_address = tp.address AND be.chain_id = tp.chain_id
                ) AS liquidity_token1

            FROM trading_pairs tp
            LEFT JOIN latest_swap ls ON tp.address = ls.pair_address AND tp.chain_id = ls.chain_id
            LEFT JOIN price_data pd ON tp.address = pd.pair_address AND tp.chain_id = pd.chain_id
            WHERE tp.address = $1 AND tp.chain_id = $2
        )
        SELECT * FROM pair_stats
        LIMIT 1
    "#;

        let result = sqlx::query_as::<_, TradingPairWithStats>(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        Ok(result)
    }

    /// 获取指定交易对的交易记录（带分页信息）
    ///
    /// # 参数
    /// * `pool` - 数据库连接池
    /// * `pair_address` - 交易对地址
    /// * `chain_id` - 链ID
    /// * `limit` - 每页记录数：
    ///   - `None`: 返回所有记录（无分页）
    ///   - `Some(n)`: 返回最多n条记录
    /// * `offset` - 分页偏移量：
    ///   - `None`: 从第一条记录开始
    ///   - `Some(n)`: 跳过前n条记录
    ///
    /// # 返回值
    /// 返回 `Result<(Vec<TradeRecord>, i64)>` 元组：
    /// - `Vec<TradeRecord>`: 查询到的交易记录列表
    /// - `i64`: 符合条件的总记录数（不考虑分页）
    pub async fn get_pair_trades(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TradeRecord>, i64), sqlx::Error> {
        let query = r#"
        WITH trades AS (
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
                -- 交易金额：转换为实际代币数量（除以精度）
                CASE 
                    WHEN tp.token0_decimals IS NOT NULL THEN
                        (se.amount0_in)::NUMERIC / POWER(10, tp.token0_decimals)::NUMERIC
                    ELSE
                        (se.amount0_in)::NUMERIC / POWER(10, 18)::NUMERIC
                END AS amount0_in,
                CASE 
                    WHEN tp.token1_decimals IS NOT NULL THEN
                        (se.amount1_in)::NUMERIC / POWER(10, tp.token1_decimals)::NUMERIC
                    ELSE
                        (se.amount1_in)::NUMERIC / POWER(10, 18)::NUMERIC
                END AS amount1_in,
                CASE 
                    WHEN tp.token0_decimals IS NOT NULL THEN
                        (se.amount0_out)::NUMERIC / POWER(10, tp.token0_decimals)::NUMERIC
                    ELSE
                        (se.amount0_out)::NUMERIC / POWER(10, 18)::NUMERIC
                END AS amount0_out,
                CASE 
                    WHEN tp.token1_decimals IS NOT NULL THEN
                        (se.amount1_out)::NUMERIC / POWER(10, tp.token1_decimals)::NUMERIC
                    ELSE
                        (se.amount1_out)::NUMERIC / POWER(10, 18)::NUMERIC
                END AS amount1_out,
                -- 价格计算：基于转换后的金额
                CASE 
                    WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN
                        ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN
                        ((se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    ELSE 0
                END::NUMERIC(38,18) AS price,
                -- 交易类型
                CASE 
                    WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN 'buy'
                    WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN 'sell'
                    ELSE 'unknown'
                END AS trade_type,
                se.block_number,
                se.timestamp
            FROM swap_events se
            LEFT JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
        )
        SELECT * FROM trades
        ORDER BY timestamp DESC
        LIMIT $3 OFFSET $4
    "#;

        let count_query = r#"
        SELECT COUNT(*) 
        FROM swap_events 
        WHERE pair_address = $1 AND chain_id = $2
    "#;

        // 获取总记录数
        let total: i64 = sqlx::query_scalar(count_query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_one(pool)
            .await?;

        // 获取分页数据
        let rows = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(limit.unwrap_or(50))
            .bind(offset.unwrap_or(0))
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
                // 所有金额都已经转换为实际代币数量
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

        Ok((trades, total))
    }

    // 获取交易对统计信息
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

    // 获取流动性事件
    pub async fn get_pair_liquidity_events(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<LiquidityRecord>, i64), sqlx::Error> {
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

        let count_query = r#"
        SELECT 
            (SELECT COUNT(*) FROM mint_events WHERE pair_address = $1 AND chain_id = $2) +
            (SELECT COUNT(*) FROM burn_events WHERE pair_address = $1 AND chain_id = $2) AS total_count
    "#;

        // 获取总记录数
        let total: i64 = sqlx::query_scalar(count_query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_one(pool)
            .await?;

        // 获取分页数据
        let rows = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(limit.unwrap_or(i32::MAX))
            .bind(offset.unwrap_or(0))
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

        Ok((liquidity_records, total))
    }

    // 修复精度处理的K线数据查询函数
    pub async fn get_kline_data(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        interval: &str,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<KLineData>, i64), sqlx::Error> {
        let config_map = IntervalConfig::get_config();
        let config = config_map
            .get(interval)
            .unwrap_or(config_map.get("1h").unwrap());

        // 构建修复精度处理的动态SQL查询
        let query = format!(
            r#"
        WITH time_series AS (
            SELECT 
                {} as time_bucket,
                -- 修复价格计算：加入代币精度处理，参考get_pair_detail的逻辑
                CASE 
                    WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN
                        ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN
                        ((se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    ELSE 0
                END as price,
                -- 修复成交量计算：加入代币精度处理
                CASE 
                    WHEN se.amount0_in > 0 THEN 
                        (se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                    WHEN se.amount1_in > 0 THEN 
                        (se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                    ELSE 0
                END as volume,
                se.timestamp,
                ROW_NUMBER() OVER (PARTITION BY {} ORDER BY se.timestamp ASC) as rn_first,
                ROW_NUMBER() OVER (PARTITION BY {} ORDER BY se.timestamp DESC) as rn_last
            FROM swap_events se
            -- 必须JOIN trading_pairs表来获取代币精度信息
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND se.timestamp >= NOW() - INTERVAL '{}'
            AND (
                (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                (se.amount1_in > 0 AND se.amount0_out > 0)
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
            COALESCE(open_price, 0)::NUMERIC(38,18) as open,
            COALESCE(high_price, 0)::NUMERIC(38,18) as high,
            COALESCE(low_price, 0)::NUMERIC(38,18) as low,
            COALESCE(close_price, 0)::NUMERIC(38,18) as close,
            COALESCE(total_volume, 0)::NUMERIC(38,18) as volume,
            COALESCE(trade_count, 0) as trade_count
        FROM kline_with_continuity
        ORDER BY time_bucket DESC
        LIMIT $3 OFFSET $4
    "#,
            config.time_bucket_expr,
            config.time_bucket_expr,
            config.time_bucket_expr,
            config.time_range
        );

        // 总数查询
        let count_query = format!(
            r#"
        WITH time_buckets AS (
            SELECT DISTINCT {} as time_bucket
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND se.timestamp >= NOW() - INTERVAL '{}'
            AND (
                (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                (se.amount1_in > 0 AND se.amount0_out > 0)
            )
        )
        SELECT COUNT(*) FROM time_buckets
    "#,
            config.time_bucket_expr, config.time_range
        );

        // 获取总记录数
        let total: i64 = sqlx::query_scalar(&count_query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_one(pool)
            .await?;

        // 获取分页数据
        let rows = sqlx::query(&query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(limit.unwrap_or(config.default_limit))
            .bind(offset.unwrap_or(0))
            .fetch_all(pool)
            .await?;

        let mut klines = Vec::new();
        for row in rows {
            klines.push(KLineData {
                timestamp: safe_get_datetime(&row, "timestamp"),
                open: safe_get_decimal(&row, "open"),
                high: safe_get_decimal(&row, "high"),
                low: safe_get_decimal(&row, "low"),
                close: safe_get_decimal(&row, "close"),
                volume: safe_get_decimal(&row, "volume"),
                trade_count: safe_get_i64(&row, "trade_count"),
            });
        }

        Ok((klines, total))
    }

    // 修复精度处理的时间序列数据查询
    pub async fn get_timeseries_data(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        hours: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TimeSeriesData>, i64), sqlx::Error> {
        let query = r#"
        SELECT 
            se.timestamp,
            -- 修复价格计算：加入代币精度处理
            CASE 
                WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN
                    ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                    NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN
                    ((se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                    NULLIF(((se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                ELSE 0
            END::NUMERIC(38,18) as price,
            -- 修复成交量计算：加入代币精度处理
            CASE 
                WHEN se.amount0_in > 0 THEN 
                    (se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC
                WHEN se.amount1_in > 0 THEN 
                    (se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC
                ELSE 0
            END::NUMERIC(38,18) as volume
        FROM swap_events se
        -- 必须JOIN trading_pairs表来获取代币精度信息
        JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
        WHERE se.pair_address = $1 AND se.chain_id = $2
        AND se.timestamp >= NOW() - INTERVAL '1 hour' * $3
        AND (
            (se.amount0_in > 0 AND se.amount1_out > 0) OR 
            (se.amount1_in > 0 AND se.amount0_out > 0)
        )
        AND (
            CASE 
                WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN
                    ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                    NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN
                    ((se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                    NULLIF(((se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                ELSE 0
            END
        ) > 0
        ORDER BY se.timestamp ASC
        LIMIT $4 OFFSET $5
    "#;

        let count_query = r#"
        SELECT COUNT(*) 
        FROM swap_events se
        JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
        WHERE se.pair_address = $1 AND se.chain_id = $2
        AND se.timestamp >= NOW() - INTERVAL '1 hour' * $3
        AND (
            (se.amount0_in > 0 AND se.amount1_out > 0) OR 
            (se.amount1_in > 0 AND se.amount0_out > 0)
        )
    "#;

        // 获取总记录数
        let total: i64 = sqlx::query_scalar(count_query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(hours)
            .fetch_one(pool)
            .await?;

        // 获取分页数据
        let rows = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(hours)
            .bind(limit.unwrap_or(1000))
            .bind(offset.unwrap_or(0))
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

        Ok((timeseries, total))
    }

    // 辅助函数：批量处理K线数据（如果需要在Rust中进行后处理）
    pub fn process_kline_continuity(mut klines: Vec<KLineData>) -> Vec<KLineData> {
        if klines.is_empty() {
            return klines;
        }

        // 按时间排序（升序）
        klines.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        // 处理开盘价连续性
        for i in 1..klines.len() {
            if klines[i].open == Decimal::ZERO {
                klines[i].open = klines[i - 1].close;
            }
        }

        // 重新按时间降序排列
        klines.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        klines
    }

    // 可选：添加价格验证函数
    pub fn validate_price_data(klines: &mut Vec<KLineData>) {
        for kline in klines.iter_mut() {
            // 确保OHLC数据的逻辑正确性
            if kline.high < kline.low {
                std::mem::swap(&mut kline.high, &mut kline.low);
            }

            // 确保开盘价和收盘价在高低价范围内
            if kline.open > kline.high {
                kline.high = kline.open;
            }
            if kline.open < kline.low {
                kline.low = kline.open;
            }
            if kline.close > kline.high {
                kline.high = kline.close;
            }
            if kline.close < kline.low {
                kline.low = kline.close;
            }
        }
    }
}
