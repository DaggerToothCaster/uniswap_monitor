use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub chains: HashMap<u64, ChainConfig>,
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChainConfig {
    pub name: String,
    pub rpc_url: String,
    pub factory_address: String,
    pub start_block: u64,
    pub poll_interval: u64,
    pub enabled: bool,
    pub factory_block_batch_size: u64,
    pub pair_block_batch_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DefaultConfig {
    pub factory_block_batch_size: u64,
    pub pair_block_batch_size: u64,
}
impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        dotenv::dotenv().ok();

        let mut chains = HashMap::new();
        // 加载默认配置
        let defaults = DefaultConfig {
            factory_block_batch_size: std::env::var("DEFAULT_FACTORY_BLOCK_BATCH_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()?,
            pair_block_batch_size: std::env::var("DEFAULT_PAIR_BLOCK_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()?,
        };
        // NOS
        chains.insert(
            2643,
            ChainConfig {
                name: "NOS".to_string(),
                rpc_url: std::env::var("NOS_RPC_URL")
                    .unwrap_or_else(|_| "https://rpc-mainnet.noschain.org".to_string()),
                factory_address: std::env::var("NOS_FACTORY_ADDRESS")
                    .unwrap_or_else(|_| "0x24D4B13082e4A0De789190fD55cB4565E3C4dFA5".to_string()),
                start_block: std::env::var("NOS_START_BLOCK")
                    .unwrap_or_else(|_| "1302932".to_string())
                    .parse()?,
                poll_interval: std::env::var("NOS_POLL_INTERVAL")
                    .unwrap_or_else(|_| "12".to_string())
                    .parse()?,
                enabled: std::env::var("NOS_ENABLED")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()?,
                factory_block_batch_size: std::env::var("NOS_FACTORY_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.factory_block_batch_size.to_string())
                    .parse()?,
                pair_block_batch_size: std::env::var("NOS_PAIR_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.pair_block_batch_size.to_string())
                    .parse()?,
            },
        );

        Ok(Config {
            database: DatabaseConfig {
                url: std::env::var("DATABASE_URL")
                    .unwrap_or_else(|_| "postgresql://localhost/uniswap_monitor".to_string()),
                max_connections: std::env::var("DB_MAX_CONNECTIONS")
                    .unwrap_or_else(|_| "10".to_string())
                    .parse()?,
            },
            chains,
            server: ServerConfig {
                host: std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
                port: std::env::var("SERVER_PORT")
                    .unwrap_or_else(|_| "3000".to_string())
                    .parse()?,
            },
        })
    }
}
