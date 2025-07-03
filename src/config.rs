use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub ethereum: EthereumConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EthereumConfig {
    pub rpc_url: String,
    pub factory_address: String,
    pub start_block: u64,
    pub poll_interval: u64, // 添加轮询间隔配置
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        dotenv::dotenv().ok();
        
        Ok(Config {
            database: DatabaseConfig {
                url: std::env::var("DATABASE_URL")
                    .unwrap_or_else(|_| "postgresql://localhost/uniswap_monitor".to_string()),
                max_connections: std::env::var("DB_MAX_CONNECTIONS")
                    .unwrap_or_else(|_| "10".to_string())
                    .parse()?,
            },
            ethereum: EthereumConfig {
                rpc_url: std::env::var("ETH_RPC_URL")
                    .unwrap_or_else(|_| "https://mainnet.infura.io/v3/YOUR_KEY".to_string()),
                factory_address: std::env::var("FACTORY_ADDRESS")
                    .unwrap_or_else(|_| "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".to_string()),
                start_block: std::env::var("START_BLOCK")
                    .unwrap_or_else(|_| "10000835".to_string())
                    .parse()?,
                poll_interval: std::env::var("POLL_INTERVAL")
                    .unwrap_or_else(|_| "12".to_string())
                    .parse()?,
            },
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
