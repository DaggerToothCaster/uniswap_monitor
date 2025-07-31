//! USD价值估算工具
//! 
//! 提供各种USD价值估算和转换功能

use rust_decimal::Decimal;
use num_traits::ToPrimitive;
use std::collections::HashMap;

/// USD估算结果
#[derive(Debug, Clone)]
pub struct UsdEstimationResult {
    /// 估算的USD价值
    pub usd_value: Decimal,
    /// 是否为有效估算
    pub is_valid: bool,
    /// 估算方法
    pub estimation_method: String,
    /// 置信度 (0.0 - 1.0)
    pub confidence: f64,
    /// 使用的价格信息
    pub price_source: String,
}

impl UsdEstimationResult {
    pub fn new(
        usd_value: Decimal,
        method: String,
        confidence: f64,
        price_source: String,
    ) -> Self {
        Self {
            usd_value,
            is_valid: usd_value > Decimal::ZERO,
            estimation_method: method,
            confidence,
            price_source,
        }
    }
    
    pub fn invalid() -> Self {
        Self {
            usd_value: Decimal::ZERO,
            is_valid: false,
            estimation_method: "none".to_string(),
            confidence: 0.0,
            price_source: "none".to_string(),
        }
    }
}

/// USD估算工具
pub struct UsdEstimator {
    /// 计价代币列表
    quote_tokens: Vec<String>,
}

impl Default for UsdEstimator {
    fn default() -> Self {
        Self {
            quote_tokens: vec!["KTO".to_string(), "USDT".to_string(), "NOS".to_string()],
        }
    }
}

impl UsdEstimator {
    /// 创建新的USD估算器
    pub fn new(quote_tokens: Vec<String>) -> Self {
        Self { quote_tokens }
    }
    
    /// 使用默认配置创建
    pub fn with_default_config() -> Self {
        Self::default()
    }
    
    /// 估算交易量USD价值
    pub fn estimate_trade_volume_usd(
        &self,
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
        trade_type: &str,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> UsdEstimationResult {
        match trade_type {
            "buy" => {
                if token0_has_price {
                    let usd_value = amount0_in * token0_price.unwrap_or(Decimal::ZERO);
                    UsdEstimationResult::new(
                        usd_value,
                        "buy_with_token0_price".to_string(),
                        0.9,
                        format!("token0_price: {:?}", token0_symbol),
                    )
                } else if token1_has_price {
                    let usd_value = amount1_out * token1_price.unwrap_or(Decimal::ZERO);
                    UsdEstimationResult::new(
                        usd_value,
                        "buy_with_token1_price".to_string(),
                        0.8,
                        format!("token1_price: {:?}", token1_symbol),
                    )
                } else {
                    UsdEstimationResult::invalid()
                }
            }
            "sell" => {
                if token0_has_price {
                    let usd_value = amount0_out * token0_price.unwrap_or(Decimal::ZERO);
                    UsdEstimationResult::new(
                        usd_value,
                        "sell_with_token0_price".to_string(),
                        0.9,
                        format!("token0_price: {:?}", token0_symbol),
                    )
                } else if token1_has_price {
                    let usd_value = amount1_in * token1_price.unwrap_or(Decimal::ZERO);
                    UsdEstimationResult::new(
                        usd_value,
                        "sell_with_token1_price".to_string(),
                        0.8,
                        format!("token1_price: {:?}", token1_symbol),
                    )
                } else {
                    UsdEstimationResult::invalid()
                }
            }
            _ => {
                // 其他类型交易，使用总金额估算
                let mut total_usd = Decimal::ZERO;
                let mut methods = Vec::new();
                let mut confidence = 0.0;
                
                if token0_has_price {
                    total_usd += (amount0_in + amount0_out) * token0_price.unwrap_or(Decimal::ZERO);
                    methods.push("token0_total");
                    confidence += 0.4;
                }
                
                if token1_has_price {
                    total_usd += (amount1_in + amount1_out) * token1_price.unwrap_or(Decimal::ZERO);
                    methods.push("token1_total");
                    confidence += 0.4;
                }
                
                if !methods.is_empty() {
                    UsdEstimationResult::new(
                        total_usd,
                        methods.join("_"),
                        confidence,
                        format!("mixed: {:?}, {:?}", token0_symbol, token1_symbol),
                    )
                } else {
                    UsdEstimationResult::invalid()
                }
            }
        }
    }
    
    /// 估算钱包交易价值USD
    pub fn estimate_wallet_value_usd(
        &self,
        amount0: Decimal,
        amount1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> UsdEstimationResult {
        let mut total_usd = Decimal::ZERO;
        let mut methods = Vec::new();
        let mut confidence = 0.0;
        let mut price_sources = Vec::new();
        
        if token0_has_price && amount0 > Decimal::ZERO {
            let token0_usd = amount0 * token0_price.unwrap_or(Decimal::ZERO);
            total_usd += token0_usd;
            methods.push("token0_amount");
            confidence += 0.5;
            price_sources.push(format!("token0: {:?}", token0_symbol));
        }
        
        if token1_has_price && amount1 > Decimal::ZERO {
            let token1_usd = amount1 * token1_price.unwrap_or(Decimal::ZERO);
            total_usd += token1_usd;
            methods.push("token1_amount");
            confidence += 0.5;
            price_sources.push(format!("token1: {:?}", token1_symbol));
        }
        
        if !methods.is_empty() {
            UsdEstimationResult::new(
                total_usd,
                methods.join("_"),
                f64::min(f64::max(confidence, 0.0), 1.0),
                price_sources.join(", "),
            )
        } else {
            UsdEstimationResult::invalid()
        }
    }
    
    /// 估算交易对交易量USD
    pub fn estimate_pair_volume_usd(
        &self,
        volume_token0: Decimal,
        volume_token1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> UsdEstimationResult {
        // 优先使用计价代币的价格
        if token1_has_price && self.is_quote_token(token1_symbol) {
            let usd_value = volume_token1 * token1_price.unwrap_or(Decimal::ZERO);
            return UsdEstimationResult::new(
                usd_value,
                "quote_token1_volume".to_string(),
                0.95,
                format!("quote_token1: {:?}", token1_symbol),
            );
        }
        
        if token0_has_price && self.is_quote_token(token0_symbol) {
            let usd_value = volume_token0 * token0_price.unwrap_or(Decimal::ZERO);
            return UsdEstimationResult::new(
                usd_value,
                "quote_token0_volume".to_string(),
                0.95,
                format!("quote_token0: {:?}", token0_symbol),
            );
        }
        
        // 使用任何可用的价格
        if token0_has_price {
            let usd_value = volume_token0 * token0_price.unwrap_or(Decimal::ZERO);
            UsdEstimationResult::new(
                usd_value,
                "token0_volume".to_string(),
                0.8,
                format!("token0: {:?}", token0_symbol),
            )
        } else if token1_has_price {
            let usd_value = volume_token1 * token1_price.unwrap_or(Decimal::ZERO);
            UsdEstimationResult::new(
                usd_value,
                "token1_volume".to_string(),
                0.8,
                format!("token1: {:?}", token1_symbol),
            )
        } else {
            UsdEstimationResult::invalid()
        }
    }
    
    /// 估算交易对流动性USD
    pub fn estimate_pair_liquidity_usd(
        &self,
        liquidity_token0: Decimal,
        liquidity_token1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> UsdEstimationResult {
        let mut total_liquidity_usd = Decimal::ZERO;
        let mut methods = Vec::new();
        let mut confidence = 0.0;
        let mut price_sources = Vec::new();
        
        if token0_has_price && liquidity_token0 > Decimal::ZERO {
            let token0_liquidity_usd = liquidity_token0 * token0_price.unwrap_or(Decimal::ZERO);
            total_liquidity_usd += token0_liquidity_usd;
            methods.push("token0_liquidity");
            confidence += 0.5;
            price_sources.push(format!("token0: {:?}", token0_symbol));
        }
        
        if token1_has_price && liquidity_token1 > Decimal::ZERO {
            let token1_liquidity_usd = liquidity_token1 * token1_price.unwrap_or(Decimal::ZERO);
            total_liquidity_usd += token1_liquidity_usd;
            methods.push("token1_liquidity");
            confidence += 0.5;
            price_sources.push(format!("token1: {:?}", token1_symbol));
        }
        
        if !methods.is_empty() {
            UsdEstimationResult::new(
                total_liquidity_usd,
                methods.join("_"),
                f64::min(confidence, 1.0),
                price_sources.join(", "),
            )
        } else {
            UsdEstimationResult::invalid()
        }
    }
    
    /// 估算流动性事件USD价值（特殊处理计价代币）
    pub fn estimate_liquidity_event_usd(
        &self,
        amount0: Decimal,
        amount1: Decimal,
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
        price_map: &HashMap<String, Decimal>,
    ) -> UsdEstimationResult {
        // 检查是否包含计价代币
        if let Some(symbol) = token1_symbol {
            let upper_symbol = symbol.to_uppercase();
            if self.quote_tokens.contains(&upper_symbol) {
                if let Some(&price) = price_map.get(&upper_symbol) {
                    // 使用计价代币数量的2倍作为总流动性价值
                    let usd_value = amount1 * price * Decimal::from(2);
                    return UsdEstimationResult::new(
                        usd_value,
                        "quote_token1_liquidity_doubled".to_string(),
                        0.9,
                        format!("quote_token1: {}", upper_symbol),
                    );
                }
            }
        }
        
        if let Some(symbol) = token0_symbol {
            let upper_symbol = symbol.to_uppercase();
            if self.quote_tokens.contains(&upper_symbol) {
                if let Some(&price) = price_map.get(&upper_symbol) {
                    // 使用计价代币数量的2倍作为总流动性价值
                    let usd_value = amount0 * price * Decimal::from(2);
                    return UsdEstimationResult::new(
                        usd_value,
                        "quote_token0_liquidity_doubled".to_string(),
                        0.9,
                        format!("quote_token0: {}", upper_symbol),
                    );
                }
            }
        }
        
        // 如果没有计价代币，尝试使用其他价格
        let mut total_usd = Decimal::ZERO;
        let mut methods = Vec::new();
        let mut price_sources = Vec::new();
        
        if let Some(symbol) = token0_symbol {
            let upper_symbol = symbol.to_uppercase();
            if let Some(&price) = price_map.get(&upper_symbol) {
                total_usd += amount0 * price;
                methods.push("token0_price");
                price_sources.push(format!("token0: {}", upper_symbol));
            }
        }
        
        if let Some(symbol) = token1_symbol {
            let upper_symbol = symbol.to_uppercase();
            if let Some(&price) = price_map.get(&upper_symbol) {
                total_usd += amount1 * price;
                methods.push("token1_price");
                price_sources.push(format!("token1: {}", upper_symbol));
            }
        }
        
        if !methods.is_empty() {
            UsdEstimationResult::new(
                total_usd,
                methods.join("_"),
                0.7,
                price_sources.join(", "),
            )
        } else {
            UsdEstimationResult::invalid()
        }
    }
    
    /// 批量估算USD价值
    pub fn batch_estimate_usd_values<T, F>(
        &self,
        items: &[T],
        estimation_fn: F,
    ) -> Vec<UsdEstimationResult>
    where
        F: Fn(&T) -> UsdEstimationResult,
    {
        items.iter().map(estimation_fn).collect()
    }
    
    /// 计算USD估算统计信息
    pub fn calculate_estimation_statistics(
        &self,
        results: &[UsdEstimationResult],
    ) -> UsdEstimationStatistics {
        let valid_results: Vec<_> = results.iter()
            .filter(|result| result.is_valid)
            .collect();
        
        let total_items = results.len();
        let valid_items = valid_results.len();
        
        let total_usd_value: Decimal = valid_results.iter()
            .map(|result| result.usd_value)
            .sum();
        
        let avg_usd_value = if valid_items > 0 {
            total_usd_value / Decimal::from(valid_items)
        } else {
            Decimal::ZERO
        };
        
        let avg_confidence = if valid_items > 0 {
            valid_results.iter()
                .map(|result| result.confidence)
                .sum::<f64>() / valid_items as f64
        } else {
            0.0
        };
        
        // 统计估算方法
        let mut method_counts = HashMap::new();
        for result in valid_results.iter() {
            *method_counts.entry(result.estimation_method.clone()).or_insert(0) += 1;
        }
        
        UsdEstimationStatistics {
            total_items,
            valid_items,
            total_usd_value,
            avg_usd_value,
            avg_confidence,
            method_distribution: method_counts,
        }
    }
    
    /// 检查是否为计价代币
    fn is_quote_token(&self, symbol: &Option<String>) -> bool {
        if let Some(symbol) = symbol {
            let upper_symbol = symbol.to_uppercase();
            self.quote_tokens.contains(&upper_symbol)
        } else {
            false
        }
    }
    
    /// 添加计价代币
    pub fn add_quote_token(&mut self, token: String) {
        let upper_token = token.to_uppercase();
        if !self.quote_tokens.contains(&upper_token) {
            self.quote_tokens.push(upper_token);
        }
    }
    
    /// 获取计价代币列表
    pub fn get_quote_tokens(&self) -> &[String] {
        &self.quote_tokens
    }
}

/// USD估算统计信息
#[derive(Debug, Clone)]
pub struct UsdEstimationStatistics {
    pub total_items: usize,
    pub valid_items: usize,
    pub total_usd_value: Decimal,
    pub avg_usd_value: Decimal,
    pub avg_confidence: f64,
    pub method_distribution: HashMap<String, usize>,
}

impl UsdEstimationStatistics {
    /// 计算有效率
    pub fn validity_rate(&self) -> f64 {
        if self.total_items > 0 {
            self.valid_items as f64 / self.total_items as f64
        } else {
            0.0
        }
    }
    
    /// 获取最常用的估算方法
    pub fn most_common_method(&self) -> Option<String> {
        self.method_distribution.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(method, _)| method.clone())
    }
}
