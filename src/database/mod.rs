pub mod operations;
pub mod utils;
use anyhow::Result;
use sqlx::PgPool;
// Re-export operations and utils
pub use operations::*;
pub use utils::*;
use tracing::debug;


#[derive(Clone)]
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
        debug!("创建数据表 create_tables");
        // let (tables, indexes, views) = tokio::join!(
        //     SystemOperations::create_tables(&self.pool),
        //     SystemOperations::create_indexes(&self.pool),
        //     SystemOperations::create_views(&self.pool),
        // );

        // tables?;
        // indexes?;
        // views?;
        // Ok(())
        let (tables, indexes) = tokio::join!(
            SystemOperations::create_tables(&self.pool),
            SystemOperations::create_indexes(&self.pool),
        );

        tables?;
        indexes?;
        Ok(())
    }
}
