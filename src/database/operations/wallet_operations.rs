use crate::database::utils::*;
use crate::types::*;
use crate::types::WalletTransaction;
use anyhow::Result;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
pub struct WalletOperations;

impl WalletOperations {
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
            where_clause,
            param_count + 1,
            param_count + 2
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
    pub async fn get_wallet_portfolio(
        pool: &PgPool,
        chain_id: Option<i32>,
        wallet_address: &str,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>, sqlx::Error> {
        // This is a placeholder implementation
        // In a real implementation, you would calculate the current token balances
        // based on all swap and liquidity events for this wallet
        Ok(vec![])
    }

    pub async fn get_wallet_pnl(
        pool: &PgPool,
        chain_id: Option<i32>,
        wallet_address: &str,
    ) -> Result<HashMap<String, serde_json::Value>, sqlx::Error> {
        // This is a placeholder implementation
        // In a real implementation, you would calculate profit/loss
        // based on entry/exit prices and current token values
        let mut pnl = HashMap::new();
        pnl.insert(
            "realized_pnl".to_string(),
            serde_json::Value::String("0".to_string()),
        );
        pnl.insert(
            "unrealized_pnl".to_string(),
            serde_json::Value::String("0".to_string()),
        );
        pnl.insert(
            "total_pnl".to_string(),
            serde_json::Value::String("0".to_string()),
        );
        Ok(pnl)
    }
}
