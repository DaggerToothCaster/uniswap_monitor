//! 价格计算工具
//! 
//! 提供各种价格计算功能，包括交易价格、USD价格等

use rust_decimal::Decimal;
use std::collections::HashMap;

/// 价格计算结果
#[derive(Debug, Clone)]
pub struct PriceCalculationResult {
    /// 计算得到的价格
    pub price: Decimal,
    /// 是否为有效价格
    pub is_valid: bool,
    /// 计算方法
    pub calculation_method: String,
    /// 置信度 (0.0 - 1.0)
    pub confidence: f64,
}

impl PriceCalculationResult {
    pub fn new(price: Decimal, method: String, confidence: f64) -> Self {
        Self {
            price,
            is_valid: price > Decimal::ZERO,
            calculation_method: method,
            confidence,
        }
    }
    
    pub fn invalid() -> Self {
        Self {
            price: Decimal::ZERO,
            is_valid: false,
            calculation_method: "none".to_string(),
            confidence: 0.0,
        }
    }
}

/// 价格计算工具
pub struct PriceCalculator;

impl PriceCalculator {
    /// 从交易金额计算价格
    /// 
    /// # 参数
    /// * `amount0_in` - token0输入数量
    /// * `amount1_in` - token1输入数量  
    /// * `amount0_out` - token0输出数量
    /// * `amount1_out` - token1输出数量
    /// 
    /// # 返回
    /// 返回 token1/token0 的价格比率
    pub fn calculate_price_from_amounts(
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
    ) -> PriceCalculationResult {
        // 买入交易: token0 -> token1
        if amount0_in > Decimal::ZERO && amount1_out > Decimal::ZERO {
            let price = amount0_in / amount1_out;
            return PriceCalculationResult::new(
                price,
                "buy_transaction".to_string(),
                0.9,
            );
        }
        
        // 卖出交易: token1 -> token0
        if amount1_in > Decimal::ZERO && amount0_out > Decimal::ZERO {
            let price = amount0_out / amount1_in;
            return PriceCalculationResult::new(
                price,
                "sell_transaction".to_string(),
                0.9,
            );
        }
        
        PriceCalculationResult::invalid()
    }
    
    /// 从原始金额计算价格（考虑代币精度）
    pub fn calculate_price_from_raw_amounts(
        raw_amount0_in: i64,
        raw_amount1_in: i64,
        raw_amount0_out: i64,
        raw_amount1_out: i64,
        token0_decimals: i32,
        token1_decimals: i32,
    ) -> PriceCalculationResult {
        let token0_divisor = Decimal::from(10_i64.pow(token0_decimals as u32));
        let token1_divisor = Decimal::from(10_i64.pow(token1_decimals as u32));

        let amount0_in = Decimal::from(raw_amount0_in) / token0_divisor;
        let amount1_in = Decimal::from(raw_amount1_in) / token1_divisor;
        let amount0_out = Decimal::from(raw_amount0_out) / token0_divisor;
        let amount1_out = Decimal::from(raw_amount1_out) / token1_divisor;

        Self::calculate_price_from_amounts(amount0_in, amount1_in, amount0_out, amount1_out)
    }
    
    /// 计算USD价格
    /// 
    /// # 参数
    /// * `token_price` - 代币相对价格
    /// * `token0_usd_price` - token0的USD价格
    /// * `token1_usd_price` - token1的USD价格
    /// * `token0_has_price` - token0是否有USD价格
    /// * `token1_has_price` - token1是否有USD价格
    /// * `token1_symbol` - token1符号（用于判断是否为计价代币）
    pub fn calculate_usd_price(
        token_price: Decimal,
        token0_usd_price: Option<Decimal>,
        token1_usd_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token1_symbol: &Option<String>,
    ) -> PriceCalculationResult {
        // 如果token1是计价代币且有价格，直接返回其USDT价格
        if token1_has_price {
            if let Some(symbol) = token1_symbol {
                let upper_symbol = symbol.to_uppercase();
                if ["KTO", "USDT", "NOS"].contains(&upper_symbol.as_str()) {
                    let price = token1_usd_price.unwrap_or(Decimal::ZERO);
                    return PriceCalculationResult::new(
                        price,
                        "direct_quote_token".to_string(),
                        1.0,
                    );
                }
            }
        }
        
        match (token0_has_price, token1_has_price) {
            (true, false) => {
                // 只有token0有USD价格，通过交易价格计算token1的USD价格
                if token_price > Decimal::ZERO {
                    let usd_price = token_price * token0_usd_price.unwrap_or(Decimal::ZERO);
                    PriceCalculationResult::new(
                        usd_price,
                        "calculated_from_token0".to_string(),
                        0.8,
                    )
                } else {
                    PriceCalculationResult::invalid()
                }
            }
            (false, true) => {
                // 只有token1有USD价格，直接使用
                let usd_price = token1_usd_price.unwrap_or(Decimal::ZERO);
                PriceCalculationResult::new(
                    usd_price,
                    "direct_token1_price".to_string(),
                    0.9,
                )
            }
            (true, true) => {
                // 两个代币都有USD价格，优先使用token1的直接价格
                let direct_price = token1_usd_price.unwrap_or(Decimal::ZERO);
                let calculated_price = if token_price > Decimal::ZERO {
                    token_price * token0_usd_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                };

                if direct_price > Decimal::ZERO {
                    PriceCalculationResult::new(
                        direct_price,
                        "direct_token1_price_preferred".to_string(),
                        0.95,
                    )
                } else {
                    PriceCalculationResult::new(
                        calculated_price,
                        "calculated_from_token0_fallback".to_string(),
                        0.7,
                    )
                }
            }
            (false, false) => {
                // 都没有USD价格，无法计算
                PriceCalculationResult::invalid()
            }
        }
    }
    
    /// 计算价格变化百分比
    pub fn calculate_price_change_percentage(
        current_price: Decimal,
        previous_price: Decimal,
    ) -> Option<Decimal> {
        if previous_price > Decimal::ZERO && current_price >= Decimal::ZERO {
            let change = (current_price - previous_price) / previous_price * Decimal::from(100);
            Some(change)
        } else {
            None
        }
    }
    
    /// 计算加权平均价格
    pub fn calculate_weighted_average_price(
        prices_and_volumes: &[(Decimal, Decimal)],
    ) -> Option<Decimal> {
        if prices_and_volumes.is_empty() {
            return None;
        }
        
        let mut total_value = Decimal::ZERO;
        let mut total_volume = Decimal::ZERO;
        
        for (price, volume) in prices_and_volumes {
            if *price > Decimal::ZERO && *volume > Decimal::ZERO {
                total_value += price * volume;
                total_volume += volume;
            }
        }
        
        if total_volume > Decimal::ZERO {
            Some(total_value / total_volume)
        } else {
            None
        }
    }
    
    /// 计算TWAP (时间加权平均价格)
    pub fn calculate_twap(
        price_time_pairs: &[(Decimal, i64)], // (price, timestamp_seconds)
    ) -> Option<Decimal> {
        if price_time_pairs.len() < 2 {
            return None;
        }
        
        let mut weighted_sum = Decimal::ZERO;
        let mut total_time = 0i64;
        
        for i in 0..price_time_pairs.len() - 1 {
            let (price, time) = price_time_pairs[i];
            let (_, next_time) = price_time_pairs[i + 1];
            
            let time_diff = next_time - time;
            if time_diff > 0 && price > Decimal::ZERO {
                weighted_sum += price * Decimal::from(time_diff);
                total_time += time_diff;
            }
        }
        
        if total_time > 0 {
            Some(weighted_sum / Decimal::from(total_time))
        } else {
            None
        }
    }
    
    /// 批量计算价格
    pub fn batch_calculate_prices<F>(
        data: &[(i64, i64, i64, i64, i32, i32)], // (amount0_in, amount1_in, amount0_out, amount1_out, decimals0, decimals1)
        filter_fn: F,
    ) -> Vec<PriceCalculationResult>
    where
        F: Fn(&PriceCalculationResult) -> bool,
    {
        data.iter()
            .map(|(a0_in, a1_in, a0_out, a1_out, dec0, dec1)| {
                Self::calculate_price_from_raw_amounts(*a0_in, *a1_in, *a0_out, *a1_out, *dec0, *dec1)
            })
            .filter(filter_fn)
            .collect()
    }
    
    /// 价格异常检测
    pub fn detect_price_anomalies(
        prices: &[Decimal],
        threshold_multiplier: f64,
    ) -> Vec<usize> {
        if prices.len() < 3 {
            return vec![];
        }
        
        // 计算中位数
        let mut sorted_prices = prices.to_vec();
        sorted_prices.sort();
        let median = sorted_prices[sorted_prices.len() / 2];
        
        // 计算MAD (Median Absolute Deviation)
        let mut deviations: Vec<Decimal> = prices.iter()
            .map(|&price| (price - median).abs())
            .collect();
        deviations.sort();
        let mad = deviations[deviations.len() / 2];
        
        // 检测异常值
        let threshold = mad * Decimal::try_from(threshold_multiplier).unwrap_or(Decimal::from(3));
        
        prices.iter()
            .enumerate()
            .filter_map(|(i, &price)| {
                if (price - median).abs() > threshold {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_price_from_amounts() {
        // 测试买入交易
        let result = PriceCalculator::calculate_price_from_amounts(
            Decimal::from(100), // amount0_in
            Decimal::ZERO,      // amount1_in
            Decimal::ZERO,      // amount0_out
            Decimal::from(50),  // amount1_out
        );
        
        assert!(result.is_valid);
        assert_eq!(result.price, Decimal::from(2)); // 100/50 = 2
        assert_eq!(result.calculation_method, "buy_transaction");
    }

    #[test]
    fn test_calculate_price_change_percentage() {
        let change = PriceCalculator::calculate_price_change_percentage(
            Decimal::from(110),
            Decimal::from(100),
        );
        
        assert_eq!(change, Some(Decimal::from(10))); // 10% increase
    }

    #[test]
    fn test_weighted_average_price() {
        let data = vec![
            (Decimal::from(100), Decimal::from(10)), // price=100, volume=10
            (Decimal::from(200), Decimal::from(20)), // price=200, volume=20
        ];
        
        let avg = PriceCalculator::calculate_weighted_average_price(&data);
        // (100*10 + 200*20) / (10+20) = 5000/30 = 166.67
        assert!(avg.is_some());
        assert!(avg.unwrap() > Decimal::from(166) && avg.unwrap() < Decimal::from(167));
    }
}
