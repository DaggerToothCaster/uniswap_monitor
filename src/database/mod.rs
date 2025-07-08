pub mod operations;
pub mod utils;

use crate::types::*;
use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn create_tables(&self) -> Result<()> {
        operations::create_tables(&self.pool).await
    }
}

// Re-export operations
pub use operations::*;
