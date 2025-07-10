use crate::types::*;
use crate::database::utils::*;
use anyhow::Result;
use sqlx::PgPool;

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

// 获取处理状态视图
pub async fn get_processing_status(pool: &PgPool) -> Result<Vec<ProcessingStatus>> {
    let status = sqlx::query_as::<_, ProcessingStatus>(
        "SELECT * FROM processing_status ORDER BY chain_id"
    )
    .fetch_all(pool)
    .await?;

    Ok(status)
}
