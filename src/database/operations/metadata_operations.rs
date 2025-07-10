use crate::database::utils::*;
use crate::types::{CreateTokenMetadata, TokenMetadata, UpdateTokenMetadata};
use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub struct MetadataOperations;

impl MetadataOperations {
    pub async fn create_token_metadata(
        pool: &PgPool,
        metadata: &CreateTokenMetadata,
    ) -> Result<TokenMetadata> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        let tags_json = metadata
            .tags
            .as_ref()
            .map(|tags| serde_json::to_value(tags))
            .transpose()?;

        sqlx::query(
        r#"
        INSERT INTO token_metadata 
        (id, chain_id, address, symbol, name, decimals, description, website_url, logo_url, 
         twitter_url, telegram_url, discord_url, github_url, explorer_url, coingecko_id, 
         coinmarketcap_id, total_supply, max_supply, is_verified, tags, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        "#,
    )
    .bind(&id)
    .bind(metadata.chain_id)
    .bind(&metadata.address)
    .bind(&metadata.symbol)
    .bind(&metadata.name)
    .bind(metadata.decimals)
    .bind(&metadata.description)
    .bind(&metadata.website_url)
    .bind(&metadata.logo_url)
    .bind(&metadata.twitter_url)
    .bind(&metadata.telegram_url)
    .bind(&metadata.discord_url)
    .bind(&metadata.github_url)
    .bind(&metadata.explorer_url)
    .bind(&metadata.coingecko_id)
    .bind(&metadata.coinmarketcap_id)
    .bind(&metadata.total_supply)
    .bind(&metadata.max_supply)
    .bind(false) // is_verified defaults to false
    .bind(&tags_json)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

        Ok(TokenMetadata {
            id,
            chain_id: metadata.chain_id,
            address: metadata.address.clone(),
            symbol: metadata.symbol.clone(),
            name: metadata.name.clone(),
            decimals: metadata.decimals,
            description: metadata.description.clone(),
            website_url: metadata.website_url.clone(),
            logo_url: metadata.logo_url.clone(),
            twitter_url: metadata.twitter_url.clone(),
            telegram_url: metadata.telegram_url.clone(),
            discord_url: metadata.discord_url.clone(),
            github_url: metadata.github_url.clone(),
            explorer_url: metadata.explorer_url.clone(),
            coingecko_id: metadata.coingecko_id.clone(),
            coinmarketcap_id: metadata.coinmarketcap_id.clone(),
            total_supply: metadata.total_supply,
            max_supply: metadata.max_supply,
            is_verified: false,
            tags: metadata.tags.clone(),
            created_at: now,
            updated_at: now,
        })
    }

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
                twitter_url: safe_get_optional_string(&row, "twitter_url"),
                telegram_url: safe_get_optional_string(&row, "telegram_url"),
                discord_url: safe_get_optional_string(&row, "discord_url"),
                github_url: safe_get_optional_string(&row, "github_url"),
                explorer_url: safe_get_optional_string(&row, "explorer_url"),
                coingecko_id: safe_get_optional_string(&row, "coingecko_id"),
                coinmarketcap_id: safe_get_optional_string(&row, "coinmarketcap_id"),
                total_supply: safe_get_optional_decimal(&row, "total_supply"),
                max_supply: safe_get_optional_decimal(&row, "max_supply"),
                is_verified: safe_get_bool(&row, "is_verified"),
                tags,
                created_at: safe_get_datetime(&row, "created_at"),
                updated_at: safe_get_datetime(&row, "updated_at"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn update_token_metadata(
        pool: &PgPool,
        chain_id: i32,
        address: &str,
        update: &UpdateTokenMetadata,
    ) -> Result<Option<TokenMetadata>> {
        let now = Utc::now();
        let tags_json = update
            .tags
            .as_ref()
            .map(|tags| serde_json::to_value(tags))
            .transpose()?;

        let result = sqlx::query(
            r#"
        UPDATE token_metadata 
        SET symbol = COALESCE($3, symbol),
            name = COALESCE($4, name),
            decimals = COALESCE($5, decimals),
            description = COALESCE($6, description),
            website_url = COALESCE($7, website_url),
            logo_url = COALESCE($8, logo_url),
            twitter_url = COALESCE($9, twitter_url),
            telegram_url = COALESCE($10, telegram_url),
            discord_url = COALESCE($11, discord_url),
            github_url = COALESCE($12, github_url),
            explorer_url = COALESCE($13, explorer_url),
            coingecko_id = COALESCE($14, coingecko_id),
            coinmarketcap_id = COALESCE($15, coinmarketcap_id),
            total_supply = COALESCE($16, total_supply),
            max_supply = COALESCE($17, max_supply),
            tags = COALESCE($18, tags),
            updated_at = $19
        WHERE chain_id = $1 AND address = $2
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
        .bind(&update.twitter_url)
        .bind(&update.telegram_url)
        .bind(&update.discord_url)
        .bind(&update.github_url)
        .bind(&update.explorer_url)
        .bind(&update.coingecko_id)
        .bind(&update.coinmarketcap_id)
        .bind(&update.total_supply)
        .bind(&update.max_supply)
        .bind(&tags_json)
        .bind(&now)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            Self::get_token_metadata(pool, chain_id, address).await
        } else {
            Ok(None)
        }
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
        verified_only: bool,
    ) -> Result<Vec<TokenMetadata>> {
        let mut conditions = Vec::new();
        let mut param_count = 0;

        if let Some(chain_id) = chain_id {
            param_count += 1;
            conditions.push(format!("chain_id = ${}", param_count));
        }

        if verified_only {
            conditions.push("is_verified = true".to_string());
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
                twitter_url: safe_get_optional_string(&row, "twitter_url"),
                telegram_url: safe_get_optional_string(&row, "telegram_url"),
                discord_url: safe_get_optional_string(&row, "discord_url"),
                github_url: safe_get_optional_string(&row, "github_url"),
                explorer_url: safe_get_optional_string(&row, "explorer_url"),
                coingecko_id: safe_get_optional_string(&row, "coingecko_id"),
                coinmarketcap_id: safe_get_optional_string(&row, "coinmarketcap_id"),
                total_supply: safe_get_optional_decimal(&row, "total_supply"),
                max_supply: safe_get_optional_decimal(&row, "max_supply"),
                is_verified: safe_get_bool(&row, "is_verified"),
                tags,
                created_at: safe_get_datetime(&row, "created_at"),
                updated_at: safe_get_datetime(&row, "updated_at"),
            });
        }

        Ok(metadata_list)
    }

    pub async fn verify_token_metadata(
        pool: &PgPool,
        chain_id: i32,
        address: &str,
    ) -> Result<Option<TokenMetadata>> {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
        UPDATE token_metadata 
        SET is_verified = true, updated_at = $3
        WHERE chain_id = $1 AND address = $2
        "#,
        )
        .bind(chain_id)
        .bind(address)
        .bind(&now)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            Self::get_token_metadata(pool, chain_id, address).await
        } else {
            Ok(None)
        }
    }
}
