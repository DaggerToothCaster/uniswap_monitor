//! 金额转换工具
//!
//! 处理代币金额的精度转换和格式化

use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// 代币金额结构
#[derive(Debug, Clone)]
pub struct TokenAmount {
    /// 原始金额（最小单位）
    pub raw_amount: i64,
    /// 实际金额（考虑精度）
    pub actual_amount: Decimal,
    /// 代币精度
    pub decimals: i32,
    /// 代币符号
    pub symbol: Option<String>,
}

impl TokenAmount {
    /// 创建新的代币金额
    pub fn new(raw_amount: i64, decimals: i32, symbol: Option<String>) -> Self {
        let actual_amount = AmountConverter::convert_raw_to_actual(raw_amount, decimals);
        Self {
            raw_amount,
            actual_amount,
            decimals,
            symbol,
        }
    }

    /// 从实际金额创建
    pub fn from_actual(actual_amount: Decimal, decimals: i32, symbol: Option<String>) -> Self {
        let raw_amount = AmountConverter::convert_actual_to_raw(actual_amount, decimals);
        Self {
            raw_amount,
            actual_amount,
            decimals,
            symbol,
        }
    }

    /// 是否为零
    pub fn is_zero(&self) -> bool {
        self.actual_amount == Decimal::ZERO
    }

    /// 是否为正数
    pub fn is_positive(&self) -> bool {
        self.actual_amount > Decimal::ZERO
    }

    /// 格式化显示
    pub fn format(&self, precision: Option<usize>) -> String {
        let precision = precision.unwrap_or(self.decimals as usize);
        if let Some(ref symbol) = self.symbol {
            format!(
                "{:.precision$} {}",
                self.actual_amount,
                symbol,
                precision = precision
            )
        } else {
            format!("{:.precision$}", self.actual_amount, precision = precision)
        }
    }
}

/// 金额转换工具
pub struct AmountConverter;

impl AmountConverter {
    /// 将原始金额转换为实际金额（考虑精度）
    ///
    /// # 参数
    /// * `raw_amount` - 原始金额（最小单位）
    /// * `decimals` - 代币精度
    ///
    /// # 返回
    /// 实际的代币数量
    pub fn convert_raw_to_actual(raw_amount: i64, decimals: i32) -> Decimal {
        if raw_amount == 0 {
            return Decimal::ZERO;
        }

        let amount_decimal = Decimal::from(raw_amount);
        let divisor = Decimal::from(10_i64.pow(decimals as u32));
        amount_decimal / divisor
    }

    /// 将实际金额转换为原始金额
    ///
    /// # 参数
    /// * `actual_amount` - 实际金额
    /// * `decimals` - 代币精度
    ///
    /// # 返回
    /// 原始金额（最小单位）
    pub fn convert_actual_to_raw(actual_amount: Decimal, decimals: i32) -> i64 {
        let multiplier = Decimal::from(10_i64.pow(decimals as u32));
        let raw_decimal = actual_amount * multiplier;

        // 转换为i64，处理可能的溢出
        raw_decimal.to_i64().unwrap_or(0)
    }

    /// 批量转换原始金额
    pub fn batch_convert_raw_to_actual(raw_amounts: &[i64], decimals: i32) -> Vec<Decimal> {
        raw_amounts
            .iter()
            .map(|&amount| Self::convert_raw_to_actual(amount, decimals))
            .collect()
    }

    /// 创建代币金额对象
    pub fn create_token_amount(
        raw_amount: i64,
        decimals: i32,
        symbol: Option<String>,
    ) -> TokenAmount {
        TokenAmount::new(raw_amount, decimals, symbol)
    }

    /// 批量创建代币金额对象
    pub fn batch_create_token_amounts(data: &[(i64, i32, Option<String>)]) -> Vec<TokenAmount> {
        data.iter()
            .map(|(raw_amount, decimals, symbol)| {
                TokenAmount::new(*raw_amount, *decimals, symbol.clone())
            })
            .collect()
    }

    /// 转换交易金额（处理输入输出）
    pub fn convert_trade_amounts(
        raw_amount0_in: i64,
        raw_amount1_in: i64,
        raw_amount0_out: i64,
        raw_amount1_out: i64,
        token0_decimals: i32,
        token1_decimals: i32,
        token0_symbol: Option<String>,
        token1_symbol: Option<String>,
    ) -> TradeAmounts {
        TradeAmounts {
            amount0_in: TokenAmount::new(raw_amount0_in, token0_decimals, token0_symbol.clone()),
            amount1_in: TokenAmount::new(raw_amount1_in, token1_decimals, token1_symbol.clone()),
            amount0_out: TokenAmount::new(raw_amount0_out, token0_decimals, token0_symbol),
            amount1_out: TokenAmount::new(raw_amount1_out, token1_decimals, token1_symbol),
        }
    }

    /// 标准化精度（将所有金额转换为相同精度）
    pub fn normalize_precision(amounts: &[TokenAmount], target_decimals: i32) -> Vec<Decimal> {
        amounts
            .iter()
            .map(|amount| {
                if amount.decimals == target_decimals {
                    amount.actual_amount
                } else if amount.decimals < target_decimals {
                    // 需要增加精度
                    let multiplier =
                        Decimal::from(10_i64.pow((target_decimals - amount.decimals) as u32));
                    amount.actual_amount * multiplier
                } else {
                    // 需要减少精度
                    let divisor =
                        Decimal::from(10_i64.pow((amount.decimals - target_decimals) as u32));
                    amount.actual_amount / divisor
                }
            })
            .collect()
    }

    /// 计算总金额
    pub fn calculate_total_amount(amounts: &[TokenAmount]) -> Decimal {
        amounts.iter().map(|amount| amount.actual_amount).sum()
    }

    /// 按符号分组金额
    pub fn group_amounts_by_symbol(amounts: &[TokenAmount]) -> HashMap<String, Vec<TokenAmount>> {
        let mut grouped = HashMap::new();

        for amount in amounts {
            let symbol = amount
                .symbol
                .clone()
                .unwrap_or_else(|| "UNKNOWN".to_string());
            grouped
                .entry(symbol)
                .or_insert_with(Vec::new)
                .push(amount.clone());
        }

        grouped
    }

    /// 过滤有效金额（大于零）
    pub fn filter_positive_amounts(amounts: Vec<TokenAmount>) -> Vec<TokenAmount> {
        amounts
            .into_iter()
            .filter(|amount| amount.is_positive())
            .collect()
    }

    /// 格式化金额显示
    pub fn format_amount_with_symbol(
        amount: Decimal,
        symbol: &Option<String>,
        precision: Option<usize>,
    ) -> String {
        let precision = precision.unwrap_or(6);
        if let Some(symbol) = symbol {
            format!("{:.precision$} {}", amount, symbol, precision = precision)
        } else {
            format!("{:.precision$}", amount, precision = precision)
        }
    }

    /// 检查金额是否在合理范围内
    pub fn validate_amount_range(
        amount: &TokenAmount,
        min_amount: Option<Decimal>,
        max_amount: Option<Decimal>,
    ) -> bool {
        if let Some(min) = min_amount {
            if amount.actual_amount < min {
                return false;
            }
        }

        if let Some(max) = max_amount {
            if amount.actual_amount > max {
                return false;
            }
        }

        true
    }
}

/// 交易金额结构
#[derive(Debug, Clone)]
pub struct TradeAmounts {
    pub amount0_in: TokenAmount,
    pub amount1_in: TokenAmount,
    pub amount0_out: TokenAmount,
    pub amount1_out: TokenAmount,
}

impl TradeAmounts {
    /// 获取输入金额总和
    pub fn total_input(&self) -> (Decimal, Decimal) {
        (self.amount0_in.actual_amount, self.amount1_in.actual_amount)
    }

    /// 获取输出金额总和
    pub fn total_output(&self) -> (Decimal, Decimal) {
        (
            self.amount0_out.actual_amount,
            self.amount1_out.actual_amount,
        )
    }

    /// 获取净变化
    pub fn net_change(&self) -> (Decimal, Decimal) {
        (
            self.amount0_out.actual_amount - self.amount0_in.actual_amount,
            self.amount1_out.actual_amount - self.amount1_in.actual_amount,
        )
    }

    /// 判断交易方向
    pub fn get_trade_direction(&self) -> String {
        if self.amount0_in.is_positive() && self.amount1_out.is_positive() {
            "token0_to_token1".to_string()
        } else if self.amount1_in.is_positive() && self.amount0_out.is_positive() {
            "token1_to_token0".to_string()
        } else {
            "unknown".to_string()
        }
    }
}
