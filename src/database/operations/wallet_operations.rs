use crate::database::utils::*;
use crate::types::WalletTransaction;
use crate::types::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
pub struct WalletOperations;

impl WalletOperations {
    /// 获取钱包交易记录（带分页，修复价格计算）
    pub async fn get_wallet_transactions(
        pool: &PgPool,
        wallet_address: &str,
        chain_id: Option<i32>,
        limit: Option<i32>,
        offset: Option<i32>,
        transaction_type: Option<&str>,
    ) -> Result<(Vec<WalletTransaction>, i64), sqlx::Error> {
        let mut conditions = vec!["(se.sender = $1 OR se.to_address = $1)".to_string()];
        let mut param_count = 1;

        if let Some(chain_id) = chain_id {
            param_count += 1;
            conditions.push(format!("se.chain_id = ${}", param_count));
        }

        // 根据交易类型过滤
        if let Some(tx_type) = transaction_type {
            match tx_type {
                "swap" => {
                    // swap事件已经是默认的，不需要额外过滤
                }
                "mint" | "burn" => {
                    return Ok((vec![], 0));
                }
                _ => {
                    return Ok((vec![], 0));
                }
            }
        }

        let where_clause = conditions.join(" AND ");

        // 优化的查询 - 使用CTE提高可读性
        let query = format!(
            r#"
        WITH wallet_trades AS (
            SELECT 
                se.id,
                se.chain_id,
                se.pair_address,
                tp.token0_symbol,
                tp.token1_symbol,
                tp.token0_decimals,
                tp.token1_decimals,
                se.transaction_hash,
                $1 as wallet_address,
                'swap' as transaction_type,
                -- 金额转换：除以对应的精度
                CASE 
                    WHEN tp.token0_decimals IS NOT NULL THEN
                        (se.amount0_in)::NUMERIC / POWER(10, tp.token0_decimals)::NUMERIC
                    ELSE
                        (se.amount0_in)::NUMERIC / POWER(10, 18)::NUMERIC
                END as amount0_in,
                CASE 
                    WHEN tp.token1_decimals IS NOT NULL THEN
                        (se.amount1_in)::NUMERIC / POWER(10, tp.token1_decimals)::NUMERIC
                    ELSE
                        (se.amount1_in)::NUMERIC / POWER(10, 18)::NUMERIC
                END as amount1_in,
                CASE 
                    WHEN tp.token0_decimals IS NOT NULL THEN
                        (se.amount0_out)::NUMERIC / POWER(10, tp.token0_decimals)::NUMERIC
                    ELSE
                        (se.amount0_out)::NUMERIC / POWER(10, 18)::NUMERIC
                END as amount0_out,
                CASE 
                    WHEN tp.token1_decimals IS NOT NULL THEN
                        (se.amount1_out)::NUMERIC / POWER(10, tp.token1_decimals)::NUMERIC
                    ELSE
                        (se.amount1_out)::NUMERIC / POWER(10, 18)::NUMERIC
                END as amount1_out,
                -- 价格计算
                CASE 
                    WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN
                        ((se.amount0_in)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_out)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN
                        ((se.amount0_out)::NUMERIC / POWER(10, COALESCE(tp.token0_decimals, 18))::NUMERIC) /
                        NULLIF(((se.amount1_in)::NUMERIC / POWER(10, COALESCE(tp.token1_decimals, 18))::NUMERIC), 0)
                    ELSE 0
                END::NUMERIC(38,18) as price,
                -- 交易类型
                CASE 
                    WHEN se.amount0_in > 0 AND se.amount1_out > 0 THEN 'buy'
                    WHEN se.amount1_in > 0 AND se.amount0_out > 0 THEN 'sell'
                    ELSE 'unknown'
                END as trade_type,
                se.block_number,
                se.timestamp
            FROM swap_events se
            JOIN trading_pairs tp 
                ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
            WHERE {}
        )
        SELECT * FROM wallet_trades
        ORDER BY timestamp DESC
        LIMIT ${} OFFSET ${}
        "#,
            where_clause,
            param_count + 1,
            param_count + 2
        );

        // 总数查询
        let count_query = format!(
            r#"
        SELECT COUNT(*) 
        FROM swap_events se
        JOIN trading_pairs tp 
            ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
        WHERE {}
        "#,
            where_clause
        );

        // 获取总记录数
        let mut count_query_builder = sqlx::query_scalar(&count_query).bind(wallet_address);
        if let Some(chain_id) = chain_id {
            count_query_builder = count_query_builder.bind(chain_id);
        }
        let total: i64 = count_query_builder.fetch_one(pool).await?;

        // 获取分页数据
        let mut query_builder = sqlx::query(&query).bind(wallet_address);
        if let Some(chain_id) = chain_id {
            query_builder = query_builder.bind(chain_id);
        }

        let rows = query_builder
            .bind(limit.unwrap_or(50))
            .bind(offset.unwrap_or(0))
            .fetch_all(pool)
            .await?;

        let mut transactions = Vec::new();

        for row in rows {
            let token0_decimals = safe_get_optional_i32(&row, "token0_decimals").unwrap_or(18);
            let token1_decimals = safe_get_optional_i32(&row, "token1_decimals").unwrap_or(18);

            // 所有金额都已经过精度转换
            let price = safe_get_decimal(&row, "price");
            let amount0_in = safe_get_decimal(&row, "amount0_in");
            let amount1_in = safe_get_decimal(&row, "amount1_in");
            let amount0_out = safe_get_decimal(&row, "amount0_out");
            let amount1_out = safe_get_decimal(&row, "amount1_out");

            transactions.push(WalletTransaction {
                id: safe_get_uuid(&row, "id"),
                chain_id: safe_get_i32(&row, "chain_id"),
                pair_address: safe_get_string(&row, "pair_address"),
                token0_symbol: safe_get_optional_string(&row, "token0_symbol"),
                token1_symbol: safe_get_optional_string(&row, "token1_symbol"),
                transaction_hash: safe_get_string(&row, "transaction_hash"),
                wallet_address: safe_get_string(&row, "wallet_address"),
                transaction_type: safe_get_string(&row, "transaction_type"),
                // 金额已经是实际代币数量
                amount0: amount0_in + amount0_out, // 实际token0数量
                amount1: amount1_in + amount1_out, // 实际token1数量
                token0_decimals: Some(token0_decimals),
                token1_decimals: Some(token1_decimals),
                price: Some(price),
                value_usd: None,
                block_number: safe_get_i64(&row, "block_number"),
                timestamp: safe_get_datetime(&row, "timestamp"),
            });
        }

        Ok((transactions, total))
    }

    /// 获取钱包统计信息（保持不变）
    pub async fn get_wallet_stats(
        pool: &PgPool,
        wallet_address: &str,
        chain_id: Option<i32>,
        days: i32,
    ) -> Result<Option<WalletStats>, sqlx::Error> {
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
                    COALESCE(SUM(
                        CASE 
                            WHEN se.amount0_in > 0 THEN 
                                (se.amount0_in::numeric / power(10, COALESCE(tp.token0_decimals, 18)))
                            WHEN se.amount1_in > 0 THEN 
                                (se.amount1_in::numeric / power(10, COALESCE(tp.token1_decimals, 18)))
                            ELSE 0
                        END
                    ), 0) as total_volume,
                    MIN(se.timestamp) as first_transaction,
                    MAX(se.timestamp) as last_transaction
                FROM swap_events se
                LEFT JOIN trading_pairs tp 
                    ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
                WHERE (se.sender = $1 OR se.to_address = $1)
                AND se.timestamp >= NOW() - INTERVAL '1 day' * $2
                {}
            )
            SELECT 
                $1 as wallet_address,
                {} as chain_id,
                COALESCE(total_transactions, 0) as total_transactions,
                COALESCE(total_volume, 0) as total_volume_usd,
                0::numeric as total_fees_paid,
                0::numeric as profit_loss,
                0::numeric as win_rate,
                CASE 
                    WHEN total_transactions > 0 THEN total_volume / total_transactions
                    ELSE 0
                END as avg_trade_size,
                first_transaction,
                last_transaction
            FROM wallet_activity
            "#,
            chain_filter,
            chain_id
                .map(|c| c.to_string())
                .unwrap_or_else(|| "NULL".to_string())
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
                first_transaction: safe_get_optional_datetime(&row, "first_transaction")
                    .unwrap_or_else(|| Utc::now()),
                last_transaction: safe_get_optional_datetime(&row, "last_transaction")
                    .unwrap_or_else(|| Utc::now()),
            }))
        } else {
            Ok(None)
        }
    }
}
