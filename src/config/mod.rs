use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub chains: HashMap<u64, ChainConfig>, // 保持u64作为chain_id的key
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
    pub chain_id: u64,       // 保留chain_id字段
    pub name: String,        // 保留name字段
    pub rpc_url: String,
    pub factory_address: String,
    pub start_block: u64,
    pub poll_interval: u64,
    pub enabled: bool,
    pub block_batch_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DefaultConfig {
    pub block_batch_size: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenv::dotenv().ok();

        if std::env::var("DATABASE_URL").is_err() {
            Self::print_config_help();
            return Err(anyhow::anyhow!("缺少DATABASE_URL配置"));
        }

        let defaults = DefaultConfig {
            block_batch_size: env_var_or_default("DEFAULT_BLOCK_BATCH_SIZE", 1000)?,
        };

        let chains = Self::load_configured_chains(&defaults)?;
        if chains.is_empty() {
            Self::print_config_help();
            return Err(anyhow::anyhow!("没有配置任何区块链"));
        }

        Ok(Config {
            database: DatabaseConfig {
                url: std::env::var("DATABASE_URL")?,
                max_connections: env_var_or_default("DB_MAX_CONNECTIONS", 10)?,
            },
            chains,
            server: ServerConfig {
                host: env_var_or_default("SERVER_HOST", "0.0.0.0".to_string())?,
                port: env_var_or_default("SERVER_PORT", 3000)?,
            },
            defaults,
        })
    }

    fn load_configured_chains(defaults: &DefaultConfig) -> anyhow::Result<HashMap<u64, ChainConfig>> {
        let mut chains = HashMap::new();
        
        // 定义支持的链信息 (chain_id, name, env_prefix)
        let supported_chains = [
            (2643u64, "NOS", "NOS"),
            (2559u64, "KTO", "KTO"),
            (1u64, "Ethereum", "ETH"),
        ];

        for (chain_id, name, prefix) in supported_chains {
            if Self::is_chain_configured(prefix) {
                chains.insert(
                    chain_id,
                    ChainConfig {
                        chain_id,
                        name: name.to_string(),
                        rpc_url: required_env_var(&format!("{}_RPC_URL", prefix))?,
                        factory_address: required_env_var(&format!("{}_FACTORY_ADDRESS", prefix))?,
                        start_block: env_var_or_default(&format!("{}_START_BLOCK", prefix), 0)?,
                        poll_interval: env_var_or_default(&format!("{}_POLL_INTERVAL", prefix), 12)?,
                        enabled: env_var_or_default(&format!("{}_ENABLED", prefix), false)?,
                        block_batch_size: env_var_or_default(
                            &format!("{}_BLOCK_BATCH_SIZE", prefix),
                            defaults.block_batch_size,
                        )?,
                    },
                );
            }
        }
        
        Ok(chains)
    }

    fn is_chain_configured(prefix: &str) -> bool {
        std::env::var(format!("{}_RPC_URL", prefix)).is_ok() &&
        std::env::var(format!("{}_FACTORY_ADDRESS", prefix)).is_ok()
    }

    fn print_config_help() {
        println!("\n🔧 配置指南");
        println!("{}", "=".repeat(50));
        println!("请配置以下环境变量:\n");

        println!("[必需配置]");
        println!("DATABASE_URL=postgres://user:pass@host/db");
        println!("<PREFIX>_RPC_URL=https://...");
        println!("<PREFIX>_FACTORY_ADDRESS=0x...\n");

        println!("[可选配置]");
        println!("DB_MAX_CONNECTIONS=10");
        println!("SERVER_HOST=0.0.0.0");
        println!("SERVER_PORT=3000");
        println!("DEFAULT_BLOCK_BATCH_SIZE=1000\n");

        println!("[支持的链]");
        println!("NOS (chain_id: 2643): NOS_RPC_URL, NOS_FACTORY_ADDRESS");
        println!("KTO (chain_id: <填写>): KTO_RPC_URL, KTO_FACTORY_ADDRESS");
        println!("ETH (chain_id: 1): ETH_RPC_URL, ETH_FACTORY_ADDRESS\n");

        println!("{}", "=".repeat(50));
    }
}

// 辅助函数保持不变
fn env_var_or_default<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(val) => val.parse().map_err(|e| anyhow::anyhow!("配置 {} 解析失败: {}", key, e)),
        Err(_) => Ok(default),
    }
}

fn required_env_var(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("缺少必需配置: {}", key))
}