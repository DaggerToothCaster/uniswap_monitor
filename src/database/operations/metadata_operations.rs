use crate::database::utils::*;
use crate::types::{TokenMetadata, UpdateTokenMetadata};
use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub struct MetadataOperations;

impl MetadataOperations {
    pub async fn get_token_metadata(
        pool: &PgPool,
        chain_id: i32,
        address: &str,
    ) -> Result<Option<TokenMetadata>> {
        let row = sqlx::query("SELECT * FROM token_metadata WHERE chain_id = $1 AND address = $2")
            .bind(chain_id)
            .bind(address)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let tags: Option<Vec<String>> =
                safe_get_optional_string(&row, "tags").and_then(|s| serde_json::from_str(&s).ok());

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

                total_supply: safe_get_optional_decimal(&row, "total_supply"),
                max_supply: safe_get_optional_decimal(&row, "max_supply"),

                created_at: safe_get_datetime(&row, "created_at"),
                updated_at: safe_get_datetime(&row, "updated_at"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn upsert_token_metadata(
        pool: &PgPool,
        update: &UpdateTokenMetadata,
    ) -> Result<Option<TokenMetadata>> {
        let now = Utc::now();
        let chain_id = update.chain_id;
        let address = &update.address;
        // 使用 ON CONFLICT DO UPDATE 并返回最新的数据
        let metadata = sqlx::query_as::<_, TokenMetadata>(
            r#"
        INSERT INTO token_metadata (
            chain_id, 
            address,
            symbol,
            name,
            decimals,
            description,
            website_url,
            logo_url,
            total_supply,
            max_supply,
            created_at,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
        ON CONFLICT (chain_id, address) DO UPDATE SET
            symbol = COALESCE(EXCLUDED.symbol, token_metadata.symbol),
            name = COALESCE(EXCLUDED.name, token_metadata.name),
            decimals = COALESCE(EXCLUDED.decimals, token_metadata.decimals),
            description = COALESCE(EXCLUDED.description, token_metadata.description),
            website_url = COALESCE(EXCLUDED.website_url, token_metadata.website_url),
            logo_url = COALESCE(EXCLUDED.logo_url, token_metadata.logo_url),
            total_supply = COALESCE(EXCLUDED.total_supply, token_metadata.total_supply),
            max_supply = COALESCE(EXCLUDED.max_supply, token_metadata.max_supply),
            updated_at = EXCLUDED.updated_at
        RETURNING *
        "#,
        )
        .bind(chain_id)
        .bind(address)
        .bind(&update.symbol)
        .bind(&update.name)
        .bind(&update.decimals)
        .bind(&update.description)
        .bind(&update.website_url)
        .bind(&update.logo_url)
        .bind(&update.total_supply)
        .bind(&update.max_supply)
        .bind(now)
        .fetch_one(pool) // 使用 fetch_one 确保返回一条记录
        .await?;

        Ok(Some(metadata))
    }

    pub async fn delete_token_metadata(
        pool: &PgPool,
        chain_id: i32,
        address: &str,
    ) -> Result<bool> {
        let result = sqlx::query("DELETE FROM token_metadata WHERE chain_id = $1 AND address = $2")
            .bind(chain_id)
            .bind(address)
            .execute(pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_token_metadata(
        pool: &PgPool,
        chain_id: Option<i32>,
        limit: i32,
        offset: i32,
    ) -> Result<Vec<TokenMetadata>> {
        let mut conditions = Vec::new();
        let mut param_count = 0;

        if let Some(chain_id) = chain_id {
            param_count += 1;
            conditions.push(format!("chain_id = ${}", param_count));
        }

        let where_clause = if conditions.is_empty() {
            "".to_string()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
        SELECT * FROM token_metadata
        {}
        ORDER BY updated_at DESC
        LIMIT ${} OFFSET ${}
        "#,
            where_clause,
            param_count + 1,
            param_count + 2
        );

        let mut query_builder = sqlx::query(&query);

        if let Some(chain_id) = chain_id {
            query_builder = query_builder.bind(chain_id);
        }

        let rows = query_builder
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?;

        let mut metadata_list = Vec::new();
        for row in rows {
            let tags: Option<Vec<String>> =
                safe_get_optional_string(&row, "tags").and_then(|s| serde_json::from_str(&s).ok());

            metadata_list.push(TokenMetadata {
                id: safe_get_uuid(&row, "id"),
                chain_id: safe_get_i32(&row, "chain_id"),
                address: safe_get_string(&row, "address"),
                symbol: safe_get_string(&row, "symbol"),
                name: safe_get_string(&row, "name"),
                decimals: safe_get_i32(&row, "decimals"),
                description: safe_get_optional_string(&row, "description"),
                website_url: safe_get_optional_string(&row, "website_url"),
                logo_url: safe_get_optional_string(&row, "logo_url"),

                total_supply: safe_get_optional_decimal(&row, "total_supply"),
                max_supply: safe_get_optional_decimal(&row, "max_supply"),

                created_at: safe_get_datetime(&row, "created_at"),
                updated_at: safe_get_datetime(&row, "updated_at"),
            });
        }

        Ok(metadata_list)
    }
}
