//! 代币重排序工具
//! 
//! 处理交易对中代币的重新排序，确保计价代币(KTO/USDT/NOS)作为token1

use crate::types::{TradeRecord, TradingPairWithStats, WalletTransaction};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// 计价代币配置
#[derive(Debug, Clone)]
pub struct QuoteTokenConfig {
    /// 计价代币列表，按优先级排序
    pub quote_tokens: Vec<String>,
    /// 代币优先级映射
    pub priority_map: HashMap<String, usize>,
}

impl Default for QuoteTokenConfig {
    fn default() -> Self {
        let quote_tokens = vec!["KTO".to_string(), "USDT".to_string(), "NOS".to_string()];
        let mut priority_map = HashMap::new();
        
        for (index, token) in quote_tokens.iter().enumerate() {
            priority_map.insert(token.clone(), index);
        }
        
        Self {
            quote_tokens,
            priority_map,
        }
    }
}

/// 代币重排序工具
pub struct TokenReorderingTool {
    config: QuoteTokenConfig,
}

impl Default for TokenReorderingTool {
    fn default() -> Self {
        Self {
            config: QuoteTokenConfig::default(),
        }
    }
}

impl TokenReorderingTool {
    /// 创建新的代币重排序工具
    pub fn new(config: QuoteTokenConfig) -> Self {
        Self { config }
    }
    
    /// 使用默认配置创建
    pub fn with_default_config() -> Self {
        Self::default()
    }
    
    /// 检查是否为计价代币
    pub fn is_quote_token(&self, symbol: &Option<String>) -> bool {
        if let Some(symbol) = symbol {
            let upper_symbol = symbol.to_uppercase();
            self.config.quote_tokens.contains(&upper_symbol)
        } else {
            false
        }
    }
    
    /// 获取计价代币优先级 (数字越小优先级越高)
    pub fn get_quote_priority(&self, symbol: &Option<String>) -> usize {
        if let Some(symbol) = symbol {
            let upper_symbol = symbol.to_uppercase();
            self.config.priority_map.get(&upper_symbol).copied().unwrap_or(usize::MAX)
        } else {
            usize::MAX
        }
    }
    
    /// 判断是否需要交换代币位置
    pub fn should_swap_tokens(&self, token0_symbol: &Option<String>, token1_symbol: &Option<String>) -> bool {
        let token0_is_quote = self.is_quote_token(token0_symbol);
        let token1_is_quote = self.is_quote_token(token1_symbol);
        
        if token0_is_quote && !token1_is_quote {
            // token0是计价代币而token1不是，需要交换
            true
        } else if token0_is_quote && token1_is_quote {
            // 两个都是计价代币，优先级高的作为token1
            self.get_quote_priority(token0_symbol) < self.get_quote_priority(token1_symbol)
        } else {
            // token0不是计价代币，不需要交换
            false
        }
    }
    
    /// 重新排序交易对统计数据
    pub fn reorder_trading_pair_with_stats(&self, pair: &mut TradingPairWithStats) {
        if self.should_swap_tokens(&pair.token0_symbol, &pair.token1_symbol) {
            self.swap_pair_tokens(pair);
        }
    }
    
    /// 重新排序交易记录
    pub fn reorder_trade_record(&self, trade: &mut TradeRecord) {
        if self.should_swap_tokens(&trade.token0_symbol, &trade.token1_symbol) {
            self.swap_trade_tokens(trade);
        }
    }
    
    /// 重新排序钱包交易
    pub fn reorder_wallet_transaction(&self, wallet_tx: &mut WalletTransaction) {
        if self.should_swap_tokens(&wallet_tx.token0_symbol, &wallet_tx.token1_symbol) {
            self.swap_wallet_tokens(wallet_tx);
        }
    }
    
    /// 批量重排序交易对
    pub fn batch_reorder_pairs(&self, pairs: &mut [TradingPairWithStats]) {
        for pair in pairs.iter_mut() {
            self.reorder_trading_pair_with_stats(pair);
        }
    }
    
    /// 批量重排序交易记录
    pub fn batch_reorder_trades(&self, trades: &mut [TradeRecord]) {
        for trade in trades.iter_mut() {
            self.reorder_trade_record(trade);
        }
    }
    
    /// 批量重排序钱包交易
    pub fn batch_reorder_wallet_transactions(&self, wallet_txs: &mut [WalletTransaction]) {
        for wallet_tx in wallet_txs.iter_mut() {
            self.reorder_wallet_transaction(wallet_tx);
        }
    }
    
    /// 交换交易对代币位置
    fn swap_pair_tokens(&self, pair: &mut TradingPairWithStats) {
        // 交换基本信息
        std::mem::swap(&mut pair.token0, &mut pair.token1);
        std::mem::swap(&mut pair.token0_symbol, &mut pair.token1_symbol);
        std::mem::swap(&mut pair.token0_decimals, &mut pair.token1_decimals);
        
        // 交换数量相关数据
        std::mem::swap(&mut pair.volume_24h_token0, &mut pair.volume_24h_token1);
        std::mem::swap(&mut pair.liquidity_token0, &mut pair.liquidity_token1);
        
        // 调整价格 (原价格的倒数)
        if pair.price > Decimal::ZERO {
            pair.price = Decimal::ONE / pair.price;
        }
        if pair.inverted_price > Decimal::ZERO {
            pair.inverted_price = Decimal::ONE / pair.inverted_price;
        }
    }
    
    /// 交换交易记录代币位置
    fn swap_trade_tokens(&self, trade: &mut TradeRecord) {
        // 交换代币信息
        std::mem::swap(&mut trade.token0_symbol, &mut trade.token1_symbol);
        std::mem::swap(&mut trade.token0_decimals, &mut trade.token1_decimals);
        
        // 交换金额
        std::mem::swap(&mut trade.amount0_in, &mut trade.amount1_in);
        std::mem::swap(&mut trade.amount0_out, &mut trade.amount1_out);
        
        // 调整价格
        if trade.price > Decimal::ZERO {
            trade.price = Decimal::ONE / trade.price;
        }
        
        // 调整交易类型
        trade.trade_type = match trade.trade_type.as_str() {
            "buy" => "sell".to_string(),
            "sell" => "buy".to_string(),
            other => other.to_string(),
        };
    }
    
    /// 交换钱包交易代币位置
    fn swap_wallet_tokens(&self, wallet_tx: &mut WalletTransaction) {
        // 交换代币信息
        std::mem::swap(&mut wallet_tx.token0_symbol, &mut wallet_tx.token1_symbol);
        std::mem::swap(&mut wallet_tx.token0_decimals, &mut wallet_tx.token1_decimals);
        
        // 交换金额
        std::mem::swap(&mut wallet_tx.amount0, &mut wallet_tx.amount1);
        
        // 调整价格
        if let Some(price) = wallet_tx.price {
            if price > Decimal::ZERO {
                wallet_tx.price = Some(Decimal::ONE / price);
            }
        }
    }
    
    /// 获取计价代币列表
    pub fn get_quote_tokens(&self) -> &[String] {
        &self.config.quote_tokens
    }
    
    /// 添加新的计价代币
    pub fn add_quote_token(&mut self, token: String, priority: Option<usize>) {
        let upper_token = token.to_uppercase();
        
        if !self.config.quote_tokens.contains(&upper_token) {
            let priority = priority.unwrap_or(self.config.quote_tokens.len());
            
            if priority >= self.config.quote_tokens.len() {
                self.config.quote_tokens.push(upper_token.clone());
            } else {
                self.config.quote_tokens.insert(priority, upper_token.clone());
            }
            
            // 重建优先级映射
            self.rebuild_priority_map();
        }
    }
    
    /// 重建优先级映射
    fn rebuild_priority_map(&mut self) {
        self.config.priority_map.clear();
        for (index, token) in self.config.quote_tokens.iter().enumerate() {
            self.config.priority_map.insert(token.clone(), index);
        }
    }
}
