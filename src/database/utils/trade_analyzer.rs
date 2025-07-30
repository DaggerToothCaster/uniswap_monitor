//! 交易分析工具
//! 
//! 提供交易类型判断、方向分析等功能

use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use chrono::{DateTime, Utc};


/// 交易类型
#[derive(Debug, Clone, PartialEq)]
pub enum TradeType {
    Buy,
    Sell,
    Unknown,
}

impl TradeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeType::Buy => "buy",
            TradeType::Sell => "sell",
            TradeType::Unknown => "unknown",
        }
    }
    
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "buy" => TradeType::Buy,
            "sell" => TradeType::Sell,
            _ => TradeType::Unknown,
        }
    }
}

/// 交易方向
#[derive(Debug, Clone, PartialEq)]
pub enum TradeDirection {
    Token0ToToken1,
    Token1ToToken0,
    Unknown,
}

/// 交易分析结果
#[derive(Debug, Clone)]
pub struct TradeAnalysisResult {
    pub trade_type: TradeType,
    pub trade_direction: TradeDirection,
    pub price: Decimal,
    pub volume: Decimal,
    pub is_valid: bool,
    pub confidence: f64,
}

/// 交易统计信息
#[derive(Debug, Clone)]
pub struct TradeStatistics {
    pub total_trades: usize,
    pub buy_trades: usize,
    pub sell_trades: usize,
    pub total_volume: Decimal,
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,
    pub avg_price: Decimal,
    pub price_range: (Decimal, Decimal), // (min, max)
    pub time_range: (DateTime<Utc>, DateTime<Utc>), // (start, end)
}

/// 交易分析工具
pub struct TradeAnalyzer;

impl TradeAnalyzer {
    /// 分析交易类型和方向
    pub fn analyze_trade(
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
    ) -> TradeAnalysisResult {
        let (trade_type, trade_direction, price, volume) = 
            Self::determine_trade_characteristics(amount0_in, amount1_in, amount0_out, amount1_out);
        
        let is_valid = price > Decimal::ZERO && volume > Decimal::ZERO;
        let confidence = if is_valid { 0.9 } else { 0.0 };
        
        TradeAnalysisResult {
            trade_type,
            trade_direction,
            price,
            volume,
            is_valid,
            confidence,
        }
    }
    
    /// 从原始金额分析交易
    pub fn analyze_trade_from_raw(
        raw_amount0_in: i64,
        raw_amount1_in: i64,
        raw_amount0_out: i64,
        raw_amount1_out: i64,
        token0_decimals: i32,
        token1_decimals: i32,
    ) -> TradeAnalysisResult {
        use super::AmountConverter;
        
        let amount0_in = AmountConverter::convert_raw_to_actual(raw_amount0_in, token0_decimals);
        let amount1_in = AmountConverter::convert_raw_to_actual(raw_amount1_in, token1_decimals);
        let amount0_out = AmountConverter::convert_raw_to_actual(raw_amount0_out, token0_decimals);
        let amount1_out = AmountConverter::convert_raw_to_actual(raw_amount1_out, token1_decimals);
        
        Self::analyze_trade(amount0_in, amount1_in, amount0_out, amount1_out)
    }
    
    /// 确定交易特征
    fn determine_trade_characteristics(
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
    ) -> (TradeType, TradeDirection, Decimal, Decimal) {
        if amount0_in > Decimal::ZERO && amount1_out > Decimal::ZERO {
            // token0 -> token1 (买入token1)
            let price = amount0_in / amount1_out;
            let volume = amount0_in;
            (TradeType::Buy, TradeDirection::Token0ToToken1, price, volume)
        } else if amount1_in > Decimal::ZERO && amount0_out > Decimal::ZERO {
            // token1 -> token0 (卖出token1)
            let price = amount0_out / amount1_in;
            let volume = amount1_in;
            (TradeType::Sell, TradeDirection::Token1ToToken0, price, volume)
        } else {
            (TradeType::Unknown, TradeDirection::Unknown, Decimal::ZERO, Decimal::ZERO)
        }
    }
    
    /// 批量分析交易
    pub fn batch_analyze_trades(
        trades_data: &[(Decimal, Decimal, Decimal, Decimal)], // (amount0_in, amount1_in, amount0_out, amount1_out)
    ) -> Vec<TradeAnalysisResult> {
        trades_data.iter()
            .map(|(a0_in, a1_in, a0_out, a1_out)| {
                Self::analyze_trade(*a0_in, *a1_in, *a0_out, *a1_out)
            })
            .collect()
    }
    
    /// 计算交易统计信息
    pub fn calculate_trade_statistics(
        analyses: &[TradeAnalysisResult],
        timestamps: &[DateTime<Utc>],
    ) -> Option<TradeStatistics> {
        if analyses.is_empty() || timestamps.is_empty() {
            return None;
        }
        
        let valid_analyses: Vec<_> = analyses.iter()
            .filter(|analysis| analysis.is_valid)
            .collect();
        
        if valid_analyses.is_empty() {
            return None;
        }
        
        let total_trades = valid_analyses.len();
        let buy_trades = valid_analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Buy)
            .count();
        let sell_trades = valid_analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Sell)
            .count();
        
        let total_volume: Decimal = valid_analyses.iter()
            .map(|analysis| analysis.volume)
            .sum();
        
        let buy_volume: Decimal = valid_analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Buy)
            .map(|analysis| analysis.volume)
            .sum();
        
        let sell_volume: Decimal = valid_analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Sell)
            .map(|analysis| analysis.volume)
            .sum();
        
        let avg_price = if total_trades > 0 {
            let total_price: Decimal = valid_analyses.iter()
                .map(|analysis| analysis.price)
                .sum();
            total_price / Decimal::from(total_trades)
        } else {
            Decimal::ZERO
        };
        
        let prices: Vec<Decimal> = valid_analyses.iter()
            .map(|analysis| analysis.price)
            .collect();
        let price_range = (
            prices.iter().min().copied().unwrap_or(Decimal::ZERO),
            prices.iter().max().copied().unwrap_or(Decimal::ZERO),
        );
        
        let time_range = (
            timestamps.iter().min().copied().unwrap_or_else(|| Utc::now()),
            timestamps.iter().max().copied().unwrap_or_else(|| Utc::now()),
        );
        
        Some(TradeStatistics {
            total_trades,
            buy_trades,
            sell_trades,
            total_volume,
            buy_volume,
            sell_volume,
            avg_price,
            price_range,
            time_range,
        })
    }
    
    /// 检测异常交易
    pub fn detect_anomalous_trades(
        analyses: &[TradeAnalysisResult],
        price_threshold_multiplier: f64,
        volume_threshold_multiplier: f64,
    ) -> Vec<usize> {
        if analyses.len() < 3 {
            return vec![];
        }
        
        let valid_analyses: Vec<_> = analyses.iter()
            .enumerate()
            .filter(|(_, analysis)| analysis.is_valid)
            .collect();
        
        if valid_analyses.len() < 3 {
            return vec![];
        }
        
        // 计算价格和成交量的统计信息
        let prices: Vec<Decimal> = valid_analyses.iter()
            .map(|(_, analysis)| analysis.price)
            .collect();
        let volumes: Vec<Decimal> = valid_analyses.iter()
            .map(|(_, analysis)| analysis.volume)
            .collect();
        
        let avg_price = prices.iter().sum::<Decimal>() / Decimal::from(prices.len());
        let avg_volume = volumes.iter().sum::<Decimal>() / Decimal::from(volumes.len());
        
        // 计算标准差
        let price_variance: Decimal = prices.iter()
            .map(|price| (*price - avg_price).powi(2))
            .sum::<Decimal>() / Decimal::from(prices.len());
        let price_std = price_variance.sqrt().unwrap_or(Decimal::ZERO);
        
        let volume_variance: Decimal = volumes.iter()
            .map(|volume| (*volume - avg_volume).powi(2))
            .sum::<Decimal>() / Decimal::from(volumes.len());
        let volume_std = volume_variance.sqrt().unwrap_or(Decimal::ZERO);
        
        // 检测异常
        let price_threshold = price_std * Decimal::try_from(price_threshold_multiplier).unwrap_or(Decimal::from(3));
        let volume_threshold = volume_std * Decimal::try_from(volume_threshold_multiplier).unwrap_or(Decimal::from(3));
        
        valid_analyses.iter()
            .filter_map(|(index, analysis)| {
                let price_deviation = (analysis.price - avg_price).abs();
                let volume_deviation = (analysis.volume - avg_volume).abs();
                
                if price_deviation > price_threshold || volume_deviation > volume_threshold {
                    Some(*index)
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// 计算买卖压力比
    pub fn calculate_buy_sell_pressure(
        analyses: &[TradeAnalysisResult],
    ) -> Option<Decimal> {
        let buy_volume: Decimal = analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Buy && analysis.is_valid)
            .map(|analysis| analysis.volume)
            .sum();
        
        let sell_volume: Decimal = analyses.iter()
            .filter(|analysis| analysis.trade_type == TradeType::Sell && analysis.is_valid)
            .map(|analysis| analysis.volume)
            .sum();
        
        if sell_volume > Decimal::ZERO {
            Some(buy_volume / sell_volume)
        } else if buy_volume > Decimal::ZERO {
            Some(Decimal::from(999)) // 表示极高的买入压力
        } else {
            None
        }
    }
    
    /// 计算价格影响
    pub fn calculate_price_impact(
        before_price: Decimal,
        after_price: Decimal,
    ) -> Option<Decimal> {
        if before_price > Decimal::ZERO {
            let impact = (after_price - before_price) / before_price * Decimal::from(100);
            Some(impact)
        } else {
            None
        }
    }
    
    /// 分析交易模式
    pub fn analyze_trading_patterns(
        analyses: &[TradeAnalysisResult],
        timestamps: &[DateTime<Utc>],
        window_minutes: i64,
    ) -> Vec<TradingPattern> {
        if analyses.len() != timestamps.len() || analyses.len() < 2 {
            return vec![];
        }
        
        let mut patterns = Vec::new();
        let window_duration = chrono::Duration::minutes(window_minutes);
        
        for i in 0..analyses.len() {
            let current_time = timestamps[i];
            let window_start = current_time - window_duration;
            
            // 收集窗口内的交易
            let window_trades: Vec<_> = analyses.iter()
                .zip(timestamps.iter())
                .filter(|(_, &time)| time >= window_start && time <= current_time)
                .map(|(analysis, _)| analysis)
                .collect();
            
            if window_trades.len() >= 3 {
                let pattern = Self::identify_pattern(&window_trades);
                patterns.push(TradingPattern {
                    timestamp: current_time,
                    pattern_type: pattern,
                    trade_count: window_trades.len(),
                    confidence: Self::calculate_pattern_confidence(&window_trades),
                });
            }
        }
        
        patterns
    }
    
    /// 识别交易模式
    fn identify_pattern(trades: &[&TradeAnalysisResult]) -> PatternType {
        let buy_count = trades.iter()
            .filter(|trade| trade.trade_type == TradeType::Buy)
            .count();
        let sell_count = trades.iter()
            .filter(|trade| trade.trade_type == TradeType::Sell)
            .count();
        
        let buy_ratio = buy_count as f64 / trades.len() as f64;
        
        if buy_ratio > 0.7 {
            PatternType::BuyingPressure
        } else if buy_ratio < 0.3 {
            PatternType::SellingPressure
        } else {
            PatternType::Balanced
        }
    }
    
    /// 计算模式置信度
    fn calculate_pattern_confidence(trades: &[&TradeAnalysisResult]) -> f64 {
        let valid_trades = trades.iter()
            .filter(|trade| trade.is_valid)
            .count();
        
        if trades.is_empty() {
            0.0
        } else {
            valid_trades as f64 / trades.len() as f64
        }
    }
}

/// 交易模式类型
#[derive(Debug, Clone, PartialEq)]
pub enum PatternType {
    BuyingPressure,
    SellingPressure,
    Balanced,
}

/// 交易模式
#[derive(Debug, Clone)]
pub struct TradingPattern {
    pub timestamp: DateTime<Utc>,
    pub pattern_type: PatternType,
    pub trade_count: usize,
    pub confidence: f64,
}