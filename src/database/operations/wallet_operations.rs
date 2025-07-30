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
    /// 获取钱包交易记录（简化SQL + 后处理）
    pub async fn get_wallet_transactions(
        pool: &PgPool,
        wallet_address: &str,
        chain_id: Option<i32>,
        limit: Option<i32>,
        offset: Option<i32>,
        transaction_type: Option<&str>,
    ) -> Result<(Vec<WalletTransaction>, i64), sqlx::Error> {
        // 构建简化的基础查询条件
        let mut where_conditions = vec!["(se.sender = $1 OR se.to_address = $1)".to_string()];
        let mut param_count = 1;

        if let Some(chain_id) = chain_id {
            param_count += 1;
            where_conditions.push(format!("se.chain_id = ${}", param_count));
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

        let where_clause = where_conditions.join(" AND ");

        // 简化的查询 - 只获取基础数据
        let query = format!(
            r#"
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
            JOIN trading_pairs tp 
                ON tp.address = se.pair_address AND tp.chain_id = se.chain_id
            WHERE {}
            ORDER BY se.timestamp DESC
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
            // 使用工具函数处理原始数据
            let processed_tx = Self::process_raw_wallet_transaction(&row)?;
            transactions.push(processed_tx);
        }

        // 后处理：计算USD字段和重排序
        super::TradeUsdCalculator::calculate_wallet_usd_fields(pool, &mut transactions).await?;

        Ok((transactions, total))
    }

    /// 工具函数：处理原始钱包交易数据
    fn process_raw_wallet_transaction(row: &sqlx::postgres::PgRow) -> Result<WalletTransaction, sqlx::Error> {
        let token0_decimals = safe_get_optional_i32(row, "token0_decimals").unwrap_or(18);
        let token1_decimals = safe_get_optional_i32(row, "token1_decimals").unwrap_or(18);

        // 获取原始金额
        let raw_amount0_in = safe_get_i64(row, "amount0_in");
        let raw_amount1_in = safe_get_i64(row, "amount1_in");
        let raw_amount0_out = safe_get_i64(row, "amount0_out");
        let raw_amount1_out = safe_get_i64(row, "amount1_out");

        // 转换为实际代币数量
        let amount0_in = Self::convert_token_amount(raw_amount0_in, token0_decimals);
        let amount1_in = Self::convert_token_amount(raw_amount1_in, token1_decimals);
        let amount0_out = Self::convert_token_amount(raw_amount0_out, token0_decimals);
        let amount1_out = Self::convert_token_amount(raw_amount1_out, token1_decimals);

        // 计算价格
        let price = Self::calculate_transaction_price(
            amount0_in, amount1_in, amount0_out, amount1_out
        );

        // 确定交易类型
        let trade_type = Self::determine_trade_type(
            raw_amount0_in, raw_amount1_in, raw_amount0_out, raw_amount1_out
        );

        Ok(WalletTransaction {
            id: safe_get_uuid(row, "id"),
            chain_id: safe_get_i32(row, "chain_id"),
            pair_address: safe_get_string(row, "pair_address"),
            token0_symbol: safe_get_optional_string(row, "token0_symbol"),
            token1_symbol: safe_get_optional_string(row, "token1_symbol"),
            transaction_hash: safe_get_string(row, "transaction_hash"),
            wallet_address: safe_get_string(row, "sender"), // 使用sender作为wallet_address
            transaction_type: "swap".to_string(),
            amount0: amount0_in + amount0_out, // 总的token0数量
            amount1: amount1_in + amount1_out, // 总的token1数量
            token0_decimals: Some(token0_decimals),
            token1_decimals: Some(token1_decimals),
            price: Some(price),
            volume_usd: Some(Decimal::ZERO), // 后续计算
            price_usd: Some(Decimal::ZERO),  // 后续计算
            block_number: safe_get_i64(row, "block_number"),
            timestamp: safe_get_datetime(row, "timestamp"),
            value_usd: Some(Decimal::ZERO), // 后续计算
        })
    }

    /// 工具函数：转换代币金额（考虑精度）
    fn convert_token_amount(raw_amount: i64, decimals: i32) -> Decimal {
        if raw_amount == 0 {
            return Decimal::ZERO;
        }
        
        let amount_decimal = Decimal::from(raw_amount);
        let divisor = Decimal::from(10_i64.pow(decimals as u32));
        amount_decimal / divisor
    }

    /// 工具函数：计算交易价格
    fn calculate_transaction_price(
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
    ) -> Decimal {
        if amount0_in > Decimal::ZERO && amount1_out > Decimal::ZERO {
            // buy: token0 -> token1
            amount0_in / amount1_out
        } else if amount1_in > Decimal::ZERO && amount0_out > Decimal::ZERO {
            // sell: token1 -> token0
            amount0_out / amount1_in
        } else {
            Decimal::ZERO
        }
    }

    /// 工具函数：确定交易类型
    fn determine_trade_type(
        raw_amount0_in: i64,
        raw_amount1_in: i64,
        raw_amount0_out: i64,
        raw_amount1_out: i64,
    ) -> String {
        if raw_amount0_in > 0 && raw_amount1_out > 0 {
            "buy".to_string()
        } else if raw_amount1_in > 0 && raw_amount0_out > 0 {
            "sell".to_string()
        } else {
            "unknown".to_string()
        }
    }

    /// 获取钱包统计信息（简化版本）
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

        // 简化的统计查询
        let query = format!(
            r#"
            SELECT 
                COUNT(*) as total_transactions,
                COALESCE(SUM(se.amount0_in + se.amount0_out + se.amount1_in + se.amount1_out), 0) as raw_volume,
                MIN(se.timestamp) as first_transaction,
                MAX(se.timestamp) as last_transaction
            FROM swap_events se
            WHERE (se.sender = $1 OR se.to_address = $1)
            AND se.timestamp >= NOW() - INTERVAL '1 day' * $2
            {}
            "#,
            chain_filter
        );

        let row = sqlx::query(&query)
            .bind(wallet_address)
            .bind(days)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let total_transactions = safe_get_i64(&row, "total_transactions");
            let raw_volume = safe_get_i64(&row, "raw_volume");
            
            // 使用工具函数处理统计数据
            let processed_stats = Self::process_wallet_stats(
                wallet_address,
                chain_id,
                total_transactions,
                raw_volume,
                safe_get_optional_datetime(&row, "first_transaction"),
                safe_get_optional_datetime(&row, "last_transaction"),
            );

            Ok(Some(processed_stats))
        } else {
            Ok(None)
        }
    }

    /// 工具函数：处理钱包统计数据
    fn process_wallet_stats(
        wallet_address: &str,
        chain_id: Option<i32>,
        total_transactions: i64,
        raw_volume: i64,
        first_transaction: Option<DateTime<Utc>>,
        last_transaction: Option<DateTime<Utc>>,
    ) -> WalletStats {
        // 简单的体积估算（实际应该根据具体代币精度计算）
        let estimated_volume = Decimal::from(raw_volume) / Decimal::from(10_i64.pow(18));
        
        let avg_trade_size = if total_transactions > 0 {
            estimated_volume / Decimal::from(total_transactions)
        } else {
            Decimal::ZERO
        };

        WalletStats {
            wallet_address: wallet_address.to_string(),
            chain_id,
            total_transactions,
            total_volume_usd: estimated_volume, // 这里应该后续通过USD计算器处理
            total_fees_paid: Decimal::ZERO,
            profit_loss: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            avg_trade_size,
            first_transaction: first_transaction.unwrap_or_else(|| Utc::now()),
            last_transaction: last_transaction.unwrap_or_else(|| Utc::now()),
        }
    }

    /// 工具函数：批量处理钱包交易的USD计算
    pub async fn batch_calculate_wallet_usd(
        pool: &PgPool,
        transactions: &mut [WalletTransaction],
    ) -> Result<(), sqlx::Error> {
        // 使用USD计算器处理
        super::TradeUsdCalculator::calculate_wallet_usd_fields(pool, transactions).await
    }

    /// 工具函数：按USD价值排序钱包交易
    pub fn sort_wallet_transactions_by_usd(transactions: &mut [WalletTransaction]) {
        super::TradeUsdCalculator::sort_by_usd_value(
            transactions,
            super::TradeUsdCalculator::wallet_get_usd_value,
        );
    }

    /// 工具函数：筛选有USD数据的钱包交易
    pub fn filter_wallet_transactions_with_usd(
        transactions: Vec<WalletTransaction>,
    ) -> Vec<WalletTransaction> {
        super::TradeUsdCalculator::filter_with_usd_data(
            transactions,
            super::TradeUsdCalculator::wallet_has_usd_data,
        )
    }
}