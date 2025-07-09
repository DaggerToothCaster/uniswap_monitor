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
            return Err(anyhow::anyhow!(
                "配置文件不存在，请按照上述提示创建配置文件"
            ));
        }

        if std::env::var("DATABASE_URL").is_err() {
            Self::print_config_help();
            return Err(anyhow::anyhow!(
                "缺少必要的配置项，请按照上述提示完善配置文件"
            ));
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
        if Self::is_chain_configured("NOS") {
            chains.insert(
                2643,
                ChainConfig {
                    name: "NOS".to_string(),
                    rpc_url: std::env::var("NOS_RPC_URL")
                        .unwrap_or_else(|_| "https://rpc-mainnet.noschain.org".to_string()),
                    factory_address: std::env::var("NOS_FACTORY_ADDRESS").unwrap_or_else(|_| {
                        "0x24D4B13082e4A0De789190fD55cB4565E3C4dFA5".to_string()
                    }),
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
                host: std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
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
