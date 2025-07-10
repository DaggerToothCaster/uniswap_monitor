use crate::types::{BurnEvent, LastProcessedBlock, MintEvent, SwapEvent};
use anyhow::Result;
use sqlx::PgPool;

// 事件类型常量
pub const EVENT_TYPE_FACTORY: &str = "factory";
pub const EVENT_TYPE_SWAP: &str = "swap";
pub const EVENT_TYPE_UNIFIED: &str = "unified";


pub struct EventOperations;

impl EventOperations {
    pub async fn initialize_last_processed_block(
        pool: &PgPool,
        chain_id: i32,
        event_type: &str,
        start_block: u64,
    ) -> Result<()> {
        sqlx::query(
            r#"
        INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id, event_type) DO NOTHING
        "#,
        )
        .bind(chain_id)
        .bind(event_type)
        .bind(start_block as i64)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn insert_swap_event(pool: &PgPool, event: &SwapEvent) -> Result<(), sqlx::Error> {
        sqlx::query(
        r#"
        INSERT INTO swap_events 
        (chain_id, pair_address, sender, amount0_in, amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index,
        amount1_in, amount0_out, amount1_out, to_address, block_number, transaction_hash, log_index, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
        "#,
        )
        .bind(event.chain_id)
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0_in)
        .bind(&event.amount1_in)
        .bind(&event.amount0_out)
        .bind(&event.amount1_out)
        .bind(&event.to_address)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn insert_burn_event(pool: &PgPool, event: &BurnEvent) -> Result<()> {
        sqlx::query(
        r#"
        INSERT INTO burn_events 
        (chain_id, pair_address, sender, amount0, amount1, to_address, block_number, transaction_hash, log_index, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
        "#,
        )
        .bind(event.chain_id)
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0)
        .bind(&event.amount1)
        .bind(&event.to_address)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn insert_mint_event(pool: &PgPool, event: &MintEvent) -> Result<()> {
        sqlx::query(
        r#"
        INSERT INTO mint_events 
        (chain_id, pair_address, sender, amount0, amount1, block_number, transaction_hash, log_index, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (chain_id, transaction_hash, log_index) DO NOTHING
        "#,
        )
        .bind(event.chain_id)
        .bind(&event.pair_address)
        .bind(&event.sender)
        .bind(&event.amount0)
        .bind(&event.amount1)
        .bind(event.block_number)
        .bind(&event.transaction_hash)
        .bind(event.log_index)
        .bind(event.timestamp)
        .execute(pool)
        .await?;

        Ok(())
    }
    pub async fn get_last_processed_block(
        pool: &PgPool,
        chain_id: i32,
        event_type: &str,
    ) -> Result<u64> {
        let result = sqlx::query_scalar::<_, i64>(
        "SELECT last_block_number FROM last_processed_blocks WHERE chain_id = $1 AND event_type = $2"
        )
        .bind(chain_id)
        .bind(event_type)
        .fetch_optional(pool)
        .await?;

        Ok(result.unwrap_or(0) as u64)
    }

    pub async fn update_last_processed_block(
        pool: &PgPool,
        chain_id: i32,
        event_type: &str,
        block_number: u64,
    ) -> Result<()> {
        sqlx::query(
            r#"
        INSERT INTO last_processed_blocks (chain_id, event_type, last_block_number)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id, event_type) 
        DO UPDATE SET 
            last_block_number = $3,
            updated_at = NOW()
        "#,
        )
        .bind(chain_id)
        .bind(event_type)
        .bind(block_number as i64)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_all_last_processed_blocks(pool: &PgPool) -> Result<Vec<LastProcessedBlock>> {
        let blocks = sqlx::query_as::<_, LastProcessedBlock>(
            "SELECT * FROM last_processed_blocks ORDER BY chain_id, event_type",
        )
        .fetch_all(pool)
        .await?;

        Ok(blocks)
    }
}
