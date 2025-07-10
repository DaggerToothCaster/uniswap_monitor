pub mod operations;
pub mod utils;
use sqlx::PgPool;
use anyhow::Result;
// Re-export operations and utils
pub use operations::*;
pub use utils::*;

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
        SystemOperations::create_tables(&self.pool).await
    }
}
