use crate::database::utils::*;
use crate::types::{
    KLineData, LiquidityRecord, PairDetail, PairStats, TimeSeriesData, TradeRecord, TradingPair,
};
use anyhow::Result;
use sqlx::{PgPool, Row};
pub struct TradingOperations;

impl TradingOperations {
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
                "SELECT * FROM trading_pairs WHERE chain_id = $1 ORDER BY created_at DESC",
            )
            .bind(chain_id)
        } else {
            sqlx::query_as::<_, TradingPair>("SELECT * FROM trading_pairs ORDER BY created_at DESC")
        };

        let pairs = query.fetch_all(pool).await?;
        Ok(pairs)
    }

    pub async fn get_pair_detail(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Option<PairDetail>> {
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

    // 交易记录查询 - 包含代币精度信息
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

    // K线数据查询 - 正确处理价格计算逻辑
    pub async fn get_kline_data(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        interval: &str,
        limit: i32,
    ) -> Result<Vec<KLineData>> {
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
                "1 day",
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
                "3 days",
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
                "7 days",
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
                "14 days",
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
                "30 days",
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
                "90 days",
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
                "1 year",
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
                "2 years",
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
                "5 years",
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
                "all time",
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
                "30 days",
            ),
        };

        let rows = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .bind(limit)
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

        Ok(klines)
    }

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

    // 获取流动性事件
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
}
