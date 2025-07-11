use crate::database::utils::*;
use crate::types::{ChainStats,SystemHealth,EventListenerStatus};
use anyhow::Result;
use sqlx::{postgres::PgRow, PgPool, Row};
use std::collections::HashMap;

pub struct StatsOperations;

impl StatsOperations {
    pub async fn get_chain_stats(pool: &PgPool, chain_id: Option<i32>) -> Result<Vec<ChainStats>> {
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
                    WHEN se.created_at >= NOW() - INTERVAL '24 hours' THEN tp.address 
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

    pub async fn get_processing_status(
        pool: &PgPool,
        chain_id: Option<i32>,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>, sqlx::Error> {
        let mut query_builder = sqlx::QueryBuilder::new(
            r#"
            SELECT chain_id, contract_type, last_block_number, updated_at
            FROM last_processed_blocks
            "#,
        );

        if let Some(chain_id) = chain_id {
            query_builder.push(" WHERE chain_id = ");
            query_builder.push_bind(chain_id);
        }

        query_builder.push(" ORDER BY chain_id, contract_type");

        let query = query_builder.build();
        let rows = query.fetch_all(pool).await?;

        let mut status_list = Vec::new();
        for row in rows {
            let mut status = HashMap::new();
            status.insert(
                "chain_id".to_string(),
                serde_json::Value::Number(serde_json::Number::from(row.get::<i32, _>("chain_id"))),
            );
            status.insert(
                "contract_type".to_string(),
                serde_json::Value::String(row.get::<String, _>("contract_type")),
            );
            status.insert(
                "last_block_number".to_string(),
                serde_json::Value::Number(serde_json::Number::from(
                    row.get::<i64, _>("last_block_number"),
                )),
            );
            status.insert(
                "updated_at".to_string(),
                serde_json::Value::String(
                    row.get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
                        .to_rfc3339(),
                ),
            );
            status_list.push(status);
        }

        Ok(status_list)
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
        let overall_status =
            if db_status == "healthy" && event_listeners.iter().all(|l| l.status == "healthy") {
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
            blocks_behind: 0,  // 需要从外部获取当前区块高度来计算
            uptime_seconds: 0, // 需要从应用启动时间计算
        })
    }
}
