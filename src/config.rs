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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        dotenv::dotenv().ok();
        
        let mut chains = HashMap::new();
        
        // Ethereum Mainnet
        chains.insert(1, ChainConfig {
            name: "Ethereum".to_string(),
            rpc_url: std::env::var("ETH_RPC_URL")
                .unwrap_or_else(|_| "https://mainnet.infura.io/v3/YOUR_KEY".to_string()),
            factory_address: std::env::var("ETH_FACTORY_ADDRESS")
                .unwrap_or_else(|_| "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".to_string()),
            start_block: std::env::var("ETH_START_BLOCK")
                .unwrap_or_else(|_| "10000835".to_string())
                .parse()?,
            poll_interval: std::env::var("ETH_POLL_INTERVAL")
                .unwrap_or_else(|_| "12".to_string())
                .parse()?,
            enabled: std::env::var("ETH_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()?,
        });

        // BSC
        chains.insert(56, ChainConfig {
            name: "BSC".to_string(),
            rpc_url: std::env::var("BSC_RPC_URL")
                .unwrap_or_else(|_| "https://bsc-dataseed1.binance.org".to_string()),
            factory_address: std::env::var("BSC_FACTORY_ADDRESS")
                .unwrap_or_else(|_| "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73".to_string()),
            start_block: std::env::var("BSC_START_BLOCK")
                .unwrap_or_else(|_| "586851".to_string())
                .parse()?,
            poll_interval: std::env::var("BSC_POLL_INTERVAL")
                .unwrap_or_else(|_| "3".to_string())
                .parse()?,
            enabled: std::env::var("BSC_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()?,
        });

        // Polygon
        chains.insert(137, ChainConfig {
            name: "Polygon".to_string(),
            rpc_url: std::env::var("POLYGON_RPC_URL")
                .unwrap_or_else(|_| "https://polygon-rpc.com".to_string()),
            factory_address: std::env::var("POLYGON_FACTORY_ADDRESS")
                .unwrap_or_else(|_| "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32".to_string()),
            start_block: std::env::var("POLYGON_START_BLOCK")
                .unwrap_or_else(|_| "4931780".to_string())
                .parse()?,
            poll_interval: std::env::var("POLYGON_POLL_INTERVAL")
                .unwrap_or_else(|_| "2".to_string())
                .parse()?,
            enabled: std::env::var("POLYGON_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()?,
        });

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
                host: std::env::var("SERVER_HOST")
                    .unwrap_or_else(|_| "0.0.0.0".to_string()),
                port: std::env::var("SERVER_PORT")
                    .unwrap_or_else(|_| "3000".to_string())
                    .parse()?,
            },
        })
    }
}
