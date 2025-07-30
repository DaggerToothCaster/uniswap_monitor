use crate::database::utils::*;
use crate::database::utils::{
    AmountConverter, DataProcessor, PriceCalculator, TokenReorderingTool, TradeAnalyzer,
    UsdEstimator, *,
};
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
/// 24小时统计数据结构
struct Pair24hStats {
    volume_token0: Decimal,
    volume_token1: Decimal,
    tx_count: i64,
    price_change: Decimal,
}

/// 流动性数据结构
struct PairLiquidity {
    token0: Decimal,
    token1: Decimal,
}
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
                time_range: "7 day",
                default_limit: 1440,
            },
        );

        config.insert("5m", IntervalConfig {
            time_bucket_expr: "date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp)::int % 5)",
            time_range: "7 days",
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

    /// 获取交易对列表（简化SQL + 后处理）
    pub async fn get_all_pairs(
        pool: &PgPool,
        chain_id: Option<i32>,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TradingPairWithStats>, i64), sqlx::Error> {
        let has_chain_filter = chain_id.unwrap_or(0) != 0;

        // 简化的基础查询 - 只获取交易对基本信息
        let base_query = if has_chain_filter {
            r#"
            SELECT 
                tp.id,
                tp.address as pair_address,
                tp.chain_id,
                tp.token0,
                tp.token1,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                tp.created_at
            FROM trading_pairs tp
            WHERE tp.chain_id = $1
            ORDER BY tp.created_at DESC
            LIMIT $2 OFFSET $3
            "#
        } else {
            r#"
            SELECT 
                tp.id,
                tp.address as pair_address,
                tp.chain_id,
                tp.token0,
                tp.token1,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                tp.created_at
            FROM trading_pairs tp
            ORDER BY tp.created_at DESC
            LIMIT $1 OFFSET $2
            "#
        };

        let rows = if has_chain_filter {
            sqlx::query(base_query)
                .bind(chain_id.unwrap())
                .bind(limit.unwrap_or(50))
                .bind(offset.unwrap_or(0))
                .fetch_all(pool)
                .await?
        } else {
            sqlx::query(base_query)
                .bind(limit.unwrap_or(50))
                .bind(offset.unwrap_or(0))
                .fetch_all(pool)
                .await?
        };

        let mut pairs = Vec::new();
        for row in rows {
            // 使用工具函数处理基础交易对数据
            let mut pair = Self::process_basic_pair_data(&row)?;
            pairs.push(pair);
        }

        // 批量计算统计数据
        Self::batch_calculate_pair_stats(pool, &mut pairs).await?;

        // 后处理：计算USD字段和重排序
        super::TradeUsdCalculator::calculate_pair_usd_fields(pool, &mut pairs).await?;

        // 按USD交易量排序
        pairs.sort_by(|a, b| {
            if a.volume_24h_usd > Decimal::ZERO || b.volume_24h_usd > Decimal::ZERO {
                b.volume_24h_usd.cmp(&a.volume_24h_usd)
            } else {
                b.volume_24h_token0.cmp(&a.volume_24h_token0)
            }
        });

        // 获取总数
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

    /// 工具函数：处理基础交易对数据
    fn process_basic_pair_data(
        row: &sqlx::postgres::PgRow,
    ) -> Result<TradingPairWithStats, sqlx::Error> {
        Ok(TradingPairWithStats {
            id: safe_get_uuid(row, "id"),
            pair_address: safe_get_string(row, "pair_address"),
            chain_id: safe_get_i32(row, "chain_id"),
            token0: safe_get_string(row, "token0"),
            token1: safe_get_string(row, "token1"),
            token0_symbol: safe_get_optional_string(row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(row, "token1_symbol"),
            token0_decimals: safe_get_optional_i32(row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(row, "token1_decimals"),
            created_at: safe_get_datetime(row, "created_at"),
            // 初始化统计字段，后续计算
            last_updated: None,
            price: Decimal::ZERO,
            inverted_price: Decimal::ZERO,
            price_24h_change: Decimal::ZERO,
            volume_24h_token0: Decimal::ZERO,
            volume_24h_token1: Decimal::ZERO,
            tx_count_24h: 0,
            liquidity_token0: Decimal::ZERO,
            liquidity_token1: Decimal::ZERO,
            price_usd: Decimal::ZERO,
            volume_24h_usd: Decimal::ZERO,
            liquidity_usd: Decimal::ZERO,
        })
    }

    /// 工具函数：批量计算交易对统计数据
    async fn batch_calculate_pair_stats(
        pool: &PgPool,
        pairs: &mut [TradingPairWithStats],
    ) -> Result<(), sqlx::Error> {
        for pair in pairs.iter_mut() {
            // 获取最新价格
            let latest_price =
                Self::get_latest_pair_price(pool, &pair.pair_address, pair.chain_id).await?;
            pair.price = latest_price;
            pair.inverted_price = if latest_price > Decimal::ZERO {
                Decimal::ONE / latest_price
            } else {
                Decimal::ZERO
            };

            // 获取24小时统计
            let stats_24h =
                Self::get_pair_24h_stats(pool, &pair.pair_address, pair.chain_id).await?;
            pair.volume_24h_token0 = stats_24h.volume_token0;
            pair.volume_24h_token1 = stats_24h.volume_token1;
            pair.tx_count_24h = stats_24h.tx_count;
            pair.price_24h_change = stats_24h.price_change;

            // 获取流动性
            let liquidity =
                Self::get_pair_liquidity(pool, &pair.pair_address, pair.chain_id).await?;
            pair.liquidity_token0 = liquidity.token0;
            pair.liquidity_token1 = liquidity.token1;
        }

        Ok(())
    }

    /// 工具函数：获取交易对最新价格
    async fn get_latest_pair_price(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Decimal, sqlx::Error> {
        let query = r#"
            SELECT 
                se.amount0_in,
                se.amount1_in,
                se.amount0_out,
                se.amount1_out,
                tp.token0_decimals,
                tp.token1_decimals
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND (
                (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                (se.amount1_in > 0 AND se.amount0_out > 0)
            )
            ORDER BY se.timestamp DESC
            LIMIT 1
        "#;

        let row = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let amount0_in = safe_get_i64(&row, "amount0_in");
            let amount1_in = safe_get_i64(&row, "amount1_in");
            let amount0_out = safe_get_i64(&row, "amount0_out");
            let amount1_out = safe_get_i64(&row, "amount1_out");
            let token0_decimals = safe_get_optional_i32(&row, "token0_decimals").unwrap_or(18);
            let token1_decimals = safe_get_optional_i32(&row, "token1_decimals").unwrap_or(18);

            // 使用工具函数计算价格
            Ok(Self::calculate_price_from_amounts(
                amount0_in,
                amount1_in,
                amount0_out,
                amount1_out,
                token0_decimals,
                token1_decimals,
            ))
        } else {
            Ok(Decimal::ZERO)
        }
    }

    /// 工具函数：从交易金额计算价格
    fn calculate_price_from_amounts(
        amount0_in: i64,
        amount1_in: i64,
        amount0_out: i64,
        amount1_out: i64,
        token0_decimals: i32,
        token1_decimals: i32,
    ) -> Decimal {
        let result = PriceCalculator::calculate_price_from_raw_amounts(
            amount0_in,
            amount1_in,
            amount0_out,
            amount1_out,
            token0_decimals,
            token1_decimals,
        );

        result.price
    }

    /// 工具函数：获取交易对24小时统计
    async fn get_pair_24h_stats(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Pair24hStats, sqlx::Error> {
        let query = r#"
            SELECT 
                COUNT(*) as tx_count,
                COALESCE(SUM(se.amount0_in + se.amount0_out), 0) as raw_volume_token0,
                COALESCE(SUM(se.amount1_in + se.amount1_out), 0) as raw_volume_token1,
                tp.token0_decimals,
                tp.token1_decimals
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND se.timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY tp.token0_decimals, tp.token1_decimals
        "#;

        let row = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let tx_count = safe_get_i64(&row, "tx_count");
            let raw_volume_token0 = safe_get_i64(&row, "raw_volume_token0");
            let raw_volume_token1 = safe_get_i64(&row, "raw_volume_token1");
            let token0_decimals = safe_get_optional_i32(&row, "token0_decimals").unwrap_or(18);
            let token1_decimals = safe_get_optional_i32(&row, "token1_decimals").unwrap_or(18);

            // 转换为实际代币数量
            let volume_token0 = Decimal::from(raw_volume_token0)
                / Decimal::from(10_i64.pow(token0_decimals as u32));
            let volume_token1 = Decimal::from(raw_volume_token1)
                / Decimal::from(10_i64.pow(token1_decimals as u32));

            // TODO: 计算价格变化（需要获取24小时前的价格）
            let price_change = Decimal::ZERO;

            Ok(Pair24hStats {
                volume_token0,
                volume_token1,
                tx_count,
                price_change,
            })
        } else {
            Ok(Pair24hStats {
                volume_token0: Decimal::ZERO,
                volume_token1: Decimal::ZERO,
                tx_count: 0,
                price_change: Decimal::ZERO,
            })
        }
    }

    /// 工具函数：获取交易对流动性
    async fn get_pair_liquidity(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<PairLiquidity, sqlx::Error> {
        let query = r#"
            SELECT 
                COALESCE(
                    (SELECT SUM(me.amount0) FROM mint_events me 
                     WHERE me.pair_address = $1 AND me.chain_id = $2), 0
                ) -
                COALESCE(
                    (SELECT SUM(be.amount0) FROM burn_events be 
                     WHERE be.pair_address = $1 AND be.chain_id = $2), 0
                ) as net_liquidity_token0,
                COALESCE(
                    (SELECT SUM(me.amount1) FROM mint_events me 
                     WHERE me.pair_address = $1 AND me.chain_id = $2), 0
                ) -
                COALESCE(
                    (SELECT SUM(be.amount1) FROM burn_events be 
                     WHERE be.pair_address = $1 AND be.chain_id = $2), 0
                ) as net_liquidity_token1,
                tp.token0_decimals,
                tp.token1_decimals
            FROM trading_pairs tp
            WHERE tp.address = $1 AND tp.chain_id = $2
        "#;

        let row = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let raw_liquidity_token0 = safe_get_i64(&row, "net_liquidity_token0");
            let raw_liquidity_token1 = safe_get_i64(&row, "net_liquidity_token1");
            let token0_decimals = safe_get_optional_i32(&row, "token0_decimals").unwrap_or(18);
            let token1_decimals = safe_get_optional_i32(&row, "token1_decimals").unwrap_or(18);

            // 转换为实际代币数量
            let liquidity_token0 = Decimal::from(raw_liquidity_token0)
                / Decimal::from(10_i64.pow(token0_decimals as u32));
            let liquidity_token1 = Decimal::from(raw_liquidity_token1)
                / Decimal::from(10_i64.pow(token1_decimals as u32));

            Ok(PairLiquidity {
                token0: liquidity_token0,
                token1: liquidity_token1,
            })
        } else {
            Ok(PairLiquidity {
                token0: Decimal::ZERO,
                token1: Decimal::ZERO,
            })
        }
    }

    /// 获取交易对详情（简化版本）
    pub async fn get_pair_detail(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Option<TradingPairWithStats>, sqlx::Error> {
        // 简化查询 - 只获取基本信息
        let query = r#"
            SELECT 
                tp.id,
                tp.address as pair_address,
                tp.chain_id,
                tp.token0,
                tp.token1,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                tp.created_at
            FROM trading_pairs tp
            WHERE tp.address = $1 AND tp.chain_id = $2
        "#;

        let row = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let mut pair = Self::process_basic_pair_data(&row)?;

            // 计算统计数据
            Self::batch_calculate_pair_stats(pool, std::slice::from_mut(&mut pair)).await?;

            // 计算USD字段
            super::TradeUsdCalculator::calculate_pair_usd_fields(
                pool,
                std::slice::from_mut(&mut pair),
            )
            .await?;

            Ok(Some(pair))
        } else {
            Ok(None)
        }
    }

    /// 获取交易记录（简化版本）
    pub async fn get_pair_trades(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TradeRecord>, i64), sqlx::Error> {
        // 简化查询 - 只获取基础交易数据
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
                se.block_number,
                se.timestamp
            FROM swap_events se
            LEFT JOIN trading_pairs tp ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            ORDER BY se.timestamp DESC
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
            // 使用工具函数处理原始交易数据
            let mut trade = Self::process_raw_trade_data(&row)?;
            trades.push(trade);
        }

        // 后处理：计算USD字段和重排序
        super::TradeUsdCalculator::calculate_trade_usd_fields(pool, &mut trades).await?;

        Ok((trades, total))
    }

    /// 工具函数：处理原始交易数据
    fn process_raw_trade_data(row: &sqlx::postgres::PgRow) -> Result<TradeRecord, sqlx::Error> {
        let token0_decimals = safe_get_optional_i32(row, "token0_decimals").unwrap_or(18);
        let token1_decimals = safe_get_optional_i32(row, "token1_decimals").unwrap_or(18);

        // 获取原始金额
        let raw_amount0_in = safe_get_i64(row, "amount0_in");
        let raw_amount1_in = safe_get_i64(row, "amount1_in");
        let raw_amount0_out = safe_get_i64(row, "amount0_out");
        let raw_amount1_out = safe_get_i64(row, "amount1_out");

        // 使用 AmountConverter 转换交易金额
        let trade_amounts = AmountConverter::convert_trade_amounts(
            raw_amount0_in,
            raw_amount1_in,
            raw_amount0_out,
            raw_amount1_out,
            token0_decimals,
            token1_decimals,
            safe_get_optional_string(row, "token0_symbol"),
            safe_get_optional_string(row, "token1_symbol"),
        );

        // 使用 TradeAnalyzer 分析交易
        let analysis = TradeAnalyzer::analyze_trade(
            trade_amounts.amount0_in.actual_amount,
            trade_amounts.amount1_in.actual_amount,
            trade_amounts.amount0_out.actual_amount,
            trade_amounts.amount1_out.actual_amount,
        );

        Ok(TradeRecord {
            id: safe_get_uuid(row, "id"),
            chain_id: safe_get_i32(row, "chain_id"),
            pair_address: safe_get_string(row, "pair_address"),
            token0_symbol: safe_get_optional_string(row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(row, "token1_symbol"),
            token0_decimals: safe_get_optional_i32(row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(row, "token1_decimals"),
            transaction_hash: safe_get_string(row, "transaction_hash"),
            sender: safe_get_string(row, "sender"),
            to_address: safe_get_string(row, "to_address"),
            amount0_in: trade_amounts.amount0_in.actual_amount,
            amount1_in: trade_amounts.amount1_in.actual_amount,
            amount0_out: trade_amounts.amount0_out.actual_amount,
            amount1_out: trade_amounts.amount1_out.actual_amount,
            price: analysis.price,
            trade_type: analysis.trade_type.as_str().to_string(),
            volume_usd: Some(Decimal::ZERO), // 后续计算
            price_usd: Some(Decimal::ZERO),  // 后续计算
            block_number: safe_get_i64(row, "block_number"),
            timestamp: safe_get_datetime(row, "timestamp"),
        })
    }

    /// 工具函数：转换原始金额
    fn convert_raw_amount(raw_amount: i64, decimals: i32) -> Decimal {
        AmountConverter::convert_raw_to_actual(raw_amount, decimals)
    }

    /// 工具函数：计算交易价格
    fn calculate_trade_price(
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
    ) -> Decimal {
        let result = PriceCalculator::calculate_price_from_amounts(
            amount0_in,
            amount1_in,
            amount0_out,
            amount1_out,
        );

        result.price
    }

    /// 工具函数：确定交易类型
    fn determine_trade_type_from_amounts(
        raw_amount0_in: i64,
        raw_amount1_in: i64,
        raw_amount0_out: i64,
        raw_amount1_out: i64,
    ) -> String {
        let analysis = TradeAnalyzer::analyze_trade_from_raw(
            raw_amount0_in,
            raw_amount1_in,
            raw_amount0_out,
            raw_amount1_out,
            18, // 默认精度
            18, // 默认精度
        );

        analysis.trade_type.as_str().to_string()
    }

    /// 获取交易对统计信息（简化版本）
    pub async fn get_pair_stats(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
    ) -> Result<Option<PairStats>> {
        // 简化查询
        let query = r#"
            SELECT 
                tp.address as pair_address,
                tp.chain_id,
                tp.token0_symbol,
                tp.token1_symbol
            FROM trading_pairs tp
            WHERE tp.address = $1 AND tp.chain_id = $2
        "#;

        let row = sqlx::query(query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            // 使用工具函数获取统计数据
            let latest_price = Self::get_latest_pair_price(pool, pair_address, chain_id).await?;
            let stats_24h = Self::get_pair_24h_stats(pool, pair_address, chain_id).await?;
            let liquidity = Self::get_pair_liquidity(pool, pair_address, chain_id).await?;

            Ok(Some(PairStats {
                pair_address: safe_get_string(&row, "pair_address"),
                chain_id: safe_get_i32(&row, "chain_id"),
                token0_symbol: safe_get_optional_string(&row, "token0_symbol")
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                token1_symbol: safe_get_optional_string(&row, "token1_symbol")
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                price: latest_price,
                volume_24h: stats_24h.volume_token0 + stats_24h.volume_token1,
                liquidity: liquidity.token0 + liquidity.token1,
                price_change_24h: stats_24h.price_change,
                tx_count_24h: stats_24h.tx_count,
            }))
        } else {
            Ok(None)
        }
    }

    /// 获取流动性事件（简化版本）
    pub async fn get_pair_liquidity_events(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<LiquidityRecord>, i64), sqlx::Error> {
        // 简化查询 - 分别查询mint和burn事件
        let mint_query = r#"
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
        "#;

        let burn_query = r#"
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
        "#;

        // 获取mint事件
        let mint_rows = sqlx::query(mint_query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_all(pool)
            .await?;

        // 获取burn事件
        let burn_rows = sqlx::query(burn_query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_all(pool)
            .await?;

        let mut liquidity_records = Vec::new();

        // 处理mint事件
        for row in mint_rows {
            let record = Self::process_liquidity_event_data(&row)?;
            liquidity_records.push(record);
        }

        // 处理burn事件
        for row in burn_rows {
            let record = Self::process_liquidity_event_data(&row)?;
            liquidity_records.push(record);
        }

        // 按时间排序
        liquidity_records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        // 应用分页
        let total = liquidity_records.len() as i64;
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(50) as usize;

        let paginated_records = if offset < liquidity_records.len() {
            let end = std::cmp::min(offset + limit, liquidity_records.len());
            liquidity_records[offset..end].to_vec()
        } else {
            vec![]
        };

        Ok((paginated_records, total))
    }

    /// 工具函数：处理流动性事件数据
    fn process_liquidity_event_data(
        row: &sqlx::postgres::PgRow,
    ) -> Result<LiquidityRecord, sqlx::Error> {
        let token0_decimals = safe_get_optional_i32(row, "token0_decimals").unwrap_or(18);
        let token1_decimals = safe_get_optional_i32(row, "token1_decimals").unwrap_or(18);

        // 获取原始金额并转换
        let raw_amount0 = safe_get_i64(row, "amount0");
        let raw_amount1 = safe_get_i64(row, "amount1");

        let amount0 = Self::convert_raw_amount(raw_amount0, token0_decimals);
        let amount1 = Self::convert_raw_amount(raw_amount1, token1_decimals);

        // 简单的USD价值估算（如果token1是计价代币）
        let value_usd = Self::estimate_liquidity_usd_value(
            &safe_get_optional_string(row, "token0_symbol"),
            &safe_get_optional_string(row, "token1_symbol"),
            amount0,
            amount1,
        );

        Ok(LiquidityRecord {
            id: safe_get_uuid(row, "id"),
            chain_id: safe_get_i32(row, "chain_id"),
            pair_address: safe_get_string(row, "pair_address"),
            token0_symbol: safe_get_optional_string(row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(row, "token1_symbol"),
            token0_decimals: safe_get_optional_i32(row, "token0_decimals"),
            token1_decimals: safe_get_optional_i32(row, "token1_decimals"),
            transaction_hash: safe_get_string(row, "transaction_hash"),
            sender: safe_get_string(row, "sender"),
            to_address: safe_get_optional_string(row, "to_address"),
            amount0,
            amount1,
            liquidity_type: safe_get_string(row, "liquidity_type"),
            value_usd,
            block_number: safe_get_i64(row, "block_number"),
            timestamp: safe_get_datetime(row, "timestamp"),
        })
    }

    /// 工具函数：估算流动性USD价值
    fn estimate_liquidity_usd_value(
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
        amount0: Decimal,
        amount1: Decimal,
    ) -> Option<Decimal> {
        // 使用 UsdEstimator 工具来估算流动性价值
        let estimator = UsdEstimator::with_default_config();

        // 创建一个简单的价格映射（这里假设计价代币价格为1）
        let mut price_map = std::collections::HashMap::new();

        // 为计价代币设置价格
        for quote_token in estimator.get_quote_tokens() {
            match quote_token.as_str() {
                "USDT" => price_map.insert(quote_token.clone(), Decimal::from(1)),
                "KTO" => price_map.insert(quote_token.clone(), Decimal::from(1)), // 假设KTO价格为1
                "NOS" => price_map.insert(quote_token.clone(), Decimal::from(1)), // 假设NOS价格为1
                _ => price_map.insert(quote_token.clone(), Decimal::from(1)),
            };
        }

        let result = estimator.estimate_liquidity_event_usd(
            amount0,
            amount1,
            token0_symbol,
            token1_symbol,
            &price_map,
        );

        if result.is_valid {
            Some(result.usd_value)
        } else {
            None
        }
    }

    /// 获取K线数据（简化版本）
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

        // 简化查询 - 获取原始交易数据
        let query = format!(
            r#"
            SELECT 
                {} as time_bucket,
                se.amount0_in,
                se.amount1_in,
                se.amount0_out,
                se.amount1_out,
                tp.token0_decimals,
                tp.token1_decimals,
                se.timestamp
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND se.timestamp >= NOW() - INTERVAL '{}'
            AND (
                (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                (se.amount1_in > 0 AND se.amount0_out > 0)
            )
            ORDER BY se.timestamp ASC
            "#,
            config.time_bucket_expr, config.time_range
        );

        let rows = sqlx::query(&query)
            .bind(pair_address)
            .bind(chain_id)
            .fetch_all(pool)
            .await?;

        // 使用工具函数处理K线数据
        let klines = Self::process_kline_data_from_trades(&rows, config.time_bucket_expr)?;

        // 应用分页
        let total = klines.len() as i64;
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(config.default_limit) as usize;

        let paginated_klines = if offset < klines.len() {
            let end = std::cmp::min(offset + limit, klines.len());
            klines[offset..end].to_vec()
        } else {
            vec![]
        };

        Ok((paginated_klines, total))
    }

    /// 工具函数：从交易数据处理K线数据
    fn process_kline_data_from_trades(
        rows: &[sqlx::postgres::PgRow],
        _time_bucket_expr: &str,
    ) -> Result<Vec<KLineData>, sqlx::Error> {
        use std::collections::BTreeMap;

        let mut kline_map: BTreeMap<chrono::DateTime<chrono::Utc>, Vec<Decimal>> = BTreeMap::new();

        // 处理每个交易记录
        for row in rows {
            let time_bucket = safe_get_datetime(row, "time_bucket");
            let token0_decimals = safe_get_optional_i32(row, "token0_decimals").unwrap_or(18);
            let token1_decimals = safe_get_optional_i32(row, "token1_decimals").unwrap_or(18);

            let price = Self::calculate_price_from_amounts(
                safe_get_i64(row, "amount0_in"),
                safe_get_i64(row, "amount1_in"),
                safe_get_i64(row, "amount0_out"),
                safe_get_i64(row, "amount1_out"),
                token0_decimals,
                token1_decimals,
            );

            if price > Decimal::ZERO {
                kline_map
                    .entry(time_bucket)
                    .or_insert_with(Vec::new)
                    .push(price);
            }
        }

        // 生成K线数据
        let mut klines = Vec::new();
        for (timestamp, prices) in kline_map {
            if !prices.is_empty() {
                let open = prices[0];
                let close = prices[prices.len() - 1];
                let high = prices.iter().max().copied().unwrap_or(Decimal::ZERO);
                let low = prices.iter().min().copied().unwrap_or(Decimal::ZERO);
                let volume = Decimal::from(prices.len()); // 简化的成交量计算

                klines.push(KLineData {
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    trade_count: prices.len() as i64,
                });
            }
        }

        klines.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(klines)
    }

    /// 获取时间序列数据（简化版本）
    pub async fn get_timeseries_data(
        pool: &PgPool,
        pair_address: &str,
        chain_id: i32,
        hours: i32,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<(Vec<TimeSeriesData>, i64), sqlx::Error> {
        // 简化查询
        let query = r#"
            SELECT 
                se.timestamp,
                se.amount0_in,
                se.amount1_in,
                se.amount0_out,
                se.amount1_out,
                tp.token0_decimals,
                tp.token1_decimals
            FROM swap_events se
            JOIN trading_pairs tp ON se.pair_address = tp.address AND se.chain_id = tp.chain_id
            WHERE se.pair_address = $1 AND se.chain_id = $2
            AND se.timestamp >= NOW() - INTERVAL '1 hour' * $3
            AND (
                (se.amount0_in > 0 AND se.amount1_out > 0) OR 
                (se.amount1_in > 0 AND se.amount0_out > 0)
            )
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
            // 使用工具函数处理时间序列数据
            let data_point = Self::process_timeseries_data_point(&row)?;
            timeseries.push(data_point);
        }

        Ok((timeseries, total))
    }

    /// 工具函数：处理时间序列数据点
    fn process_timeseries_data_point(
        row: &sqlx::postgres::PgRow,
    ) -> Result<TimeSeriesData, sqlx::Error> {
        let token0_decimals = safe_get_optional_i32(row, "token0_decimals").unwrap_or(18);
        let token1_decimals = safe_get_optional_i32(row, "token1_decimals").unwrap_or(18);

        let price = Self::calculate_price_from_amounts(
            safe_get_i64(row, "amount0_in"),
            safe_get_i64(row, "amount1_in"),
            safe_get_i64(row, "amount0_out"),
            safe_get_i64(row, "amount1_out"),
            token0_decimals,
            token1_decimals,
        );

        // 简化的成交量计算
        let raw_volume = safe_get_i64(row, "amount0_in") + safe_get_i64(row, "amount1_in");
        let volume = Decimal::from(raw_volume) / Decimal::from(10_i64.pow(18)); // 使用18位精度作为默认

        Ok(TimeSeriesData {
            timestamp: safe_get_datetime(row, "timestamp"),
            price,
            volume,
        })
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
}
