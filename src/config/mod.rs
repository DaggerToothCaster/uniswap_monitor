use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub chains: HashMap<u64, ChainConfig>,
    pub server: ServerConfig,
    pub defaults: DefaultConfig,
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
        if let Err(_) = dotenv::dotenv() {
            Self::print_config_help();
            return Err(anyhow::anyhow!("配置文件不存在，请按照上述提示创建配置文件"));
        }

        if std::env::var("DATABASE_URL").is_err() {
            Self::print_config_help();
            return Err(anyhow::anyhow!("缺少必要的配置项，请按照上述提示完善配置文件"));
        }

        let defaults = DefaultConfig {
            factory_block_batch_size: std::env::var("DEFAULT_FACTORY_BLOCK_BATCH_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()?,
            pair_block_batch_size: std::env::var("DEFAULT_PAIR_BLOCK_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()?,
        };

        let mut chains = HashMap::new();
        
        // Ethereum Mainnet
        if Self::is_chain_configured("ETH") {
            chains.insert(1, ChainConfig {
                name: "Ethereum".to_string(),
                rpc_url: std::env::var("ETH_RPC_URL")?,
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
                factory_block_batch_size: std::env::var("ETH_FACTORY_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.factory_block_batch_size.to_string())
                    .parse()?,
                pair_block_batch_size: std::env::var("ETH_PAIR_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.pair_block_batch_size.to_string())
                    .parse()?,
            });
        }

        // BSC
        if Self::is_chain_configured("BSC") {
            chains.insert(56, ChainConfig {
                name: "BSC".to_string(),
                rpc_url: std::env::var("BSC_RPC_URL")?,
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
                factory_block_batch_size: std::env::var("BSC_FACTORY_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.factory_block_batch_size.to_string())
                    .parse()?,
                pair_block_batch_size: std::env::var("BSC_PAIR_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.pair_block_batch_size.to_string())
                    .parse()?,
            });
        }

        // Polygon
        if Self::is_chain_configured("POLYGON") {
            chains.insert(137, ChainConfig {
                name: "Polygon".to_string(),
                rpc_url: std::env::var("POLYGON_RPC_URL")?,
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
                factory_block_batch_size: std::env::var("POLYGON_FACTORY_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.factory_block_batch_size.to_string())
                    .parse()?,
                pair_block_batch_size: std::env::var("POLYGON_PAIR_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.pair_block_batch_size.to_string())
                    .parse()?,
            });
        }

        // Arbitrum
        if Self::is_chain_configured("ARBITRUM") {
            chains.insert(42161, ChainConfig {
                name: "Arbitrum".to_string(),
                rpc_url: std::env::var("ARBITRUM_RPC_URL")?,
                factory_address: std::env::var("ARBITRUM_FACTORY_ADDRESS")
                    .unwrap_or_else(|_| "0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9".to_string()),
                start_block: std::env::var("ARBITRUM_START_BLOCK")
                    .unwrap_or_else(|_| "150".to_string())
                    .parse()?,
                poll_interval: std::env::var("ARBITRUM_POLL_INTERVAL")
                    .unwrap_or_else(|_| "1".to_string())
                    .parse()?,
                enabled: std::env::var("ARBITRUM_ENABLED")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()?,
                factory_block_batch_size: std::env::var("ARBITRUM_FACTORY_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.factory_block_batch_size.to_string())
                    .parse()?,
                pair_block_batch_size: std::env::var("ARBITRUM_PAIR_BLOCK_BATCH_SIZE")
                    .unwrap_or_else(|_| defaults.pair_block_batch_size.to_string())
                    .parse()?,
            });
        }

        if chains.is_empty() {
            Self::print_config_help();
            return Err(anyhow::anyhow!("没有配置任何区块链，请至少配置一个区块链"));
        }

        Ok(Config {
            database: DatabaseConfig {
                url: std::env::var("DATABASE_URL")?,
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
            defaults,
        })
    }

    fn is_chain_configured(chain_prefix: &str) -> bool {
        std::env::var(format!("{}_RPC_URL", chain_prefix)).is_ok()
    }

    fn print_config_help() {
        println!("\n🔧 配置文件不存在或配置不完整！");
        println!("{}", "=".repeat(80));
        println!("请创建 .env 文件并添加以下配置：\n");
        
        println!("# 数据库配置");
        println!("DATABASE_URL=postgresql://username:password@localhost/uniswap_monitor");
        println!("DB_MAX_CONNECTIONS=10\n");
        
        println!("# 以太坊配置");
        println!("ETH_RPC_URL=https://mainnet.infura.io/v3/YOUR_INFURA_KEY");
        println!("ETH_FACTORY_ADDRESS=0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        println!("ETH_START_BLOCK=10000835");
        println!("ETH_POLL_INTERVAL=12");
        println!("ETH_ENABLED=true");
        println!("ETH_FACTORY_BLOCK_BATCH_SIZE=1000");
        println!("ETH_PAIR_BLOCK_BATCH_SIZE=100\n");
        
        println!("# 服务器配置");
        println!("SERVER_HOST=0.0.0.0");
        println!("SERVER_PORT=3000\n");
        
        println!("# 全局默认配置");
        println!("DEFAULT_FACTORY_BLOCK_BATCH_SIZE=1000");
        println!("DEFAULT_PAIR_BLOCK_BATCH_SIZE=100\n");
        
        println!("{}", "=".repeat(80));
    }
}
