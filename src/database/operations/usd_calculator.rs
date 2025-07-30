use crate::types::{TradeRecord, TradingPairWithStats, WalletTransaction};
use rust_decimal::Decimal;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::HashMap;

/// 统一的USD计算辅助结构
pub struct TradeUsdCalculator;

/// 代币重排序和定价工具
pub struct TokenReorderingTool;

impl TokenReorderingTool {
    /// 定义计价代币优先级 (KTO, USDT, NOS)
    const QUOTE_TOKENS: &'static [&'static str] = &["KTO", "USDT", "NOS"];

    /// 检查是否为计价代币
    pub fn is_quote_token(symbol: &Option<String>) -> bool {
        if let Some(symbol) = symbol {
            let upper_symbol = symbol.to_uppercase();
            Self::QUOTE_TOKENS.contains(&upper_symbol.as_str())
        } else {
            false
        }
    }

    /// 获取计价代币优先级 (数字越小优先级越高)
    pub fn get_quote_priority(symbol: &Option<String>) -> usize {
        if let Some(symbol) = symbol {
            let upper_symbol = symbol.to_uppercase();
            Self::QUOTE_TOKENS
                .iter()
                .position(|&token| token == upper_symbol)
                .unwrap_or(usize::MAX)
        } else {
            usize::MAX
        }
    }

    /// 重新排序交易对，确保计价代币作为token1
    pub fn reorder_trading_pair_with_stats(pair: &mut TradingPairWithStats) {
        let token0_is_quote = Self::is_quote_token(&pair.token0_symbol);
        let token1_is_quote = Self::is_quote_token(&pair.token1_symbol);

        // 如果token0是计价代币而token1不是，或者两个都是计价代币但token0优先级更高，则交换
        let should_swap = if token0_is_quote && !token1_is_quote {
            true
        } else if token0_is_quote && token1_is_quote {
            Self::get_quote_priority(&pair.token0_symbol)
                < Self::get_quote_priority(&pair.token1_symbol)
        } else {
            false
        };

        if should_swap {
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
    }

    /// 重新排序交易记录
    pub fn reorder_trade_record(trade: &mut TradeRecord) {
        let token0_is_quote = Self::is_quote_token(&trade.token0_symbol);
        let token1_is_quote = Self::is_quote_token(&trade.token1_symbol);

        let should_swap = if token0_is_quote && !token1_is_quote {
            true
        } else if token0_is_quote && token1_is_quote {
            Self::get_quote_priority(&trade.token0_symbol)
                < Self::get_quote_priority(&trade.token1_symbol)
        } else {
            false
        };

        if should_swap {
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
    }

    /// 重新排序钱包交易
    pub fn reorder_wallet_transaction(wallet_tx: &mut WalletTransaction) {
        let token0_is_quote = Self::is_quote_token(&wallet_tx.token0_symbol);
        let token1_is_quote = Self::is_quote_token(&wallet_tx.token1_symbol);

        let should_swap = if token0_is_quote && !token1_is_quote {
            true
        } else if token0_is_quote && token1_is_quote {
            Self::get_quote_priority(&wallet_tx.token0_symbol)
                < Self::get_quote_priority(&wallet_tx.token1_symbol)
        } else {
            false
        };

        if should_swap {
            // 交换代币信息
            std::mem::swap(&mut wallet_tx.token0_symbol, &mut wallet_tx.token1_symbol);
            std::mem::swap(
                &mut wallet_tx.token0_decimals,
                &mut wallet_tx.token1_decimals,
            );

            // 交换金额
            std::mem::swap(&mut wallet_tx.amount0, &mut wallet_tx.amount1);

            // 调整价格
            if let Some(price) = wallet_tx.price {
                if price > Decimal::ZERO {
                    wallet_tx.price = Some(Decimal::ONE / price);
                }
            }
        }
    }
}

impl TradeUsdCalculator {
    /// 获取所有代币的最新USD价格
    pub async fn get_token_prices(pool: &PgPool) -> Result<HashMap<String, Decimal>, sqlx::Error> {
        let query = r#"
            SELECT DISTINCT ON (UPPER(token_symbol)) 
                UPPER(token_symbol) as symbol,
                price_usd
            FROM token_prices 
            WHERE price_usd > 0
            ORDER BY UPPER(token_symbol), timestamp DESC
        "#;

        let rows = sqlx::query(query).fetch_all(pool).await?;

        let mut price_map = HashMap::new();
        for row in rows {
            let symbol: String = row.get("symbol");
            let price: Decimal = row.get("price_usd");
            price_map.insert(symbol, price);
        }

        Ok(price_map)
    }

    /// 获取代币价格信息
    fn get_token_price_info(
        token_symbol: &Option<String>,
        price_map: &HashMap<String, Decimal>,
    ) -> (Option<Decimal>, bool) {
        if let Some(symbol) = token_symbol {
            let upper_symbol = symbol.to_uppercase();
            if let Some(&price) = price_map.get(&upper_symbol) {
                return (Some(price), true);
            }
        }
        (None, false)
    }

    /// 为TradeRecord计算USD字段 (带重排序)
    pub async fn calculate_trade_usd_fields(
        pool: &PgPool,
        trades: &mut [TradeRecord],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        for trade in trades.iter_mut() {
            // 首先进行代币重排序
            TokenReorderingTool::reorder_trade_record(trade);

            let (token0_price, token0_has_price) =
                Self::get_token_price_info(&trade.token0_symbol, &price_map);
            let (token1_price, token1_has_price) =
                Self::get_token_price_info(&trade.token1_symbol, &price_map);

            // 计算交易量USD
            trade.volume_usd = Some(Self::calculate_trade_volume_usd(
                &trade.amount0_in,
                &trade.amount1_in,
                &trade.amount0_out,
                &trade.amount1_out,
                &trade.trade_type,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            ));

            // 计算token1的USD价格（基于交易价格）
            trade.price_usd = Some(Self::calculate_token1_usd_price(
                trade.price,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
                &trade.token1_symbol,
            ));
        }

        Ok(())
    }

    /// 为WalletTransaction计算USD字段 (带重排序)
    pub async fn calculate_wallet_usd_fields(
        pool: &PgPool,
        wallet_txs: &mut [WalletTransaction],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        if price_map.is_empty() {
            return Ok(());
        }

        for wallet_tx in wallet_txs.iter_mut() {
            // 首先进行代币重排序
            TokenReorderingTool::reorder_wallet_transaction(wallet_tx);

            let (token0_price, token0_has_price) =
                Self::get_token_price_info(&wallet_tx.token0_symbol, &price_map);
            let (token1_price, token1_has_price) =
                Self::get_token_price_info(&wallet_tx.token1_symbol, &price_map);

            // 计算交易价值USD
            let usd_value = Self::calculate_wallet_value_usd(
                wallet_tx.amount0,
                wallet_tx.amount1,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );

            wallet_tx.value_usd = Some(usd_value);

            // 计算token1的USD价格
            wallet_tx.price_usd = Some(Self::calculate_token1_usd_price(
                wallet_tx.price.unwrap_or(Decimal::ZERO),
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
                &wallet_tx.token1_symbol,
            ));
        }

        Ok(())
    }

    /// 为TradingPairWithStats计算USD字段 (带重排序)
    pub async fn calculate_pair_usd_fields(
        pool: &PgPool,
        pairs: &mut [TradingPairWithStats],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        for pair in pairs.iter_mut() {
            // 首先进行代币重排序
            TokenReorderingTool::reorder_trading_pair_with_stats(pair);

            let (token0_price, token0_has_price) =
                Self::get_token_price_info(&pair.token0_symbol, &price_map);
            let (token1_price, token1_has_price) =
                Self::get_token_price_info(&pair.token1_symbol, &price_map);

            // 计算token1的USD价格
            pair.price_usd = Self::calculate_token1_usd_price(
                pair.price,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
                &pair.token1_symbol,
            );

            // 计算24小时交易量USD
            pair.volume_24h_usd = Self::calculate_pair_volume_usd(
                pair.volume_24h_token0,
                pair.volume_24h_token1,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );

            // 计算流动性USD
            pair.liquidity_usd = Self::calculate_pair_liquidity_usd(
                pair.liquidity_token0,
                pair.liquidity_token1,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
        }

        Ok(())
    }

    /// 计算token1的USD价格（修正版本，支持计价代币直接转换）
    fn calculate_token1_usd_price(
        price: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
        token1_symbol: &Option<String>,
    ) -> Decimal {
        // 如果token1是计价代币(KTO/USDT/NOS)且有价格，直接返回其USDT价格
        if token1_has_price {
            if let Some(symbol) = token1_symbol {
                let upper_symbol = symbol.to_uppercase();
                if TokenReorderingTool::QUOTE_TOKENS.contains(&upper_symbol.as_str()) {
                    return token1_price.unwrap_or(Decimal::ZERO);
                }
            }
        }

        match (token0_has_price, token1_has_price) {
            (true, false) => {
                // 只有token0有USD价格，通过交易价格计算token1的USD价格
                if price > Decimal::ZERO {
                    price * token0_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                }
            }
            (false, true) => {
                // 只有token1有USD价格，直接使用
                token1_price.unwrap_or(Decimal::ZERO)
            }
            (true, true) => {
                // 两个代币都有USD价格，优先使用token1的直接价格
                let direct_price = token1_price.unwrap_or(Decimal::ZERO);
                let calculated_price = if price > Decimal::ZERO {
                    price * token0_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                };

                if direct_price > Decimal::ZERO {
                    direct_price
                } else {
                    calculated_price
                }
            }
            (false, false) => {
                // 都没有USD价格，无法计算
                Decimal::ZERO
            }
        }
    }

    /// 计算钱包交易价值USD
    fn calculate_wallet_value_usd(
        amount0: Decimal,
        amount1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
    ) -> Decimal {
        let mut total_usd = Decimal::ZERO;

        if token0_has_price && amount0 > Decimal::ZERO {
            let token0_usd = amount0 * token0_price.unwrap_or(Decimal::ZERO);
            total_usd += token0_usd;
        }

        if token1_has_price && amount1 > Decimal::ZERO {
            let token1_usd = amount1 * token1_price.unwrap_or(Decimal::ZERO);
            total_usd += token1_usd;
        }

        total_usd
    }

    /// 计算交易量USD
    fn calculate_trade_volume_usd(
        amount0_in: &Decimal,
        amount1_in: &Decimal,
        amount0_out: &Decimal,
        amount1_out: &Decimal,
        trade_type: &str,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
    ) -> Decimal {
        match trade_type {
            "buy" => {
                if token0_has_price {
                    amount0_in * token0_price.unwrap_or(Decimal::ZERO)
                } else if token1_has_price {
                    amount1_out * token1_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                }
            }
            "sell" => {
                if token0_has_price {
                    amount0_out * token0_price.unwrap_or(Decimal::ZERO)
                } else if token1_has_price {
                    amount1_in * token1_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                }
            }
            _ => {
                if token0_has_price {
                    (amount0_in + amount0_out) * token0_price.unwrap_or(Decimal::ZERO)
                } else if token1_has_price {
                    (amount1_in + amount1_out) * token1_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                }
            }
        }
    }

    /// 计算交易对24小时交易量USD
    fn calculate_pair_volume_usd(
        volume_token0: Decimal,
        volume_token1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
    ) -> Decimal {
        if token0_has_price {
            volume_token0 * token0_price.unwrap_or(Decimal::ZERO)
        } else if token1_has_price {
            volume_token1 * token1_price.unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        }
    }

    /// 计算交易对流动性USD
    fn calculate_pair_liquidity_usd(
        liquidity_token0: Decimal,
        liquidity_token1: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
    ) -> Decimal {
        let mut total_liquidity_usd = Decimal::ZERO;

        if token0_has_price {
            total_liquidity_usd += liquidity_token0 * token0_price.unwrap_or(Decimal::ZERO);
        }

        if token1_has_price {
            total_liquidity_usd += liquidity_token1 * token1_price.unwrap_or(Decimal::ZERO);
        }

        total_liquidity_usd
    }

    /// 筛选有USD数据的记录
    pub fn filter_with_usd_data<T>(items: Vec<T>, has_usd_fn: fn(&T) -> bool) -> Vec<T> {
        items.into_iter().filter(has_usd_fn).collect()
    }

    /// 按USD值排序
    pub fn sort_by_usd_value<T>(items: &mut [T], get_usd_fn: fn(&T) -> Decimal) {
        items.sort_by(|a, b| get_usd_fn(b).cmp(&get_usd_fn(a)));
    }
}

/// USD统计信息
#[derive(Debug, Clone)]
pub struct UsdStats {
    pub total_items: usize,
    pub items_with_usd_data: usize,
    pub total_value_usd: Decimal,
    pub avg_value_usd: Decimal,
}

impl UsdStats {
    pub fn calculate<T>(
        items: &[T],
        get_usd_fn: fn(&T) -> Decimal,
        has_usd_fn: fn(&T) -> bool,
    ) -> Self {
        let items_with_usd: Vec<_> = items.iter().filter(|item| has_usd_fn(item)).collect();
        let total_value_usd: Decimal = items_with_usd.iter().map(|item| get_usd_fn(item)).sum();
        let avg_value_usd = if !items_with_usd.is_empty() {
            total_value_usd / Decimal::from(items_with_usd.len())
        } else {
            Decimal::ZERO
        };

        Self {
            total_items: items.len(),
            items_with_usd_data: items_with_usd.len(),
            total_value_usd,
            avg_value_usd,
        }
    }
}

// 辅助函数用于不同类型的USD数据检查和获取
impl TradeUsdCalculator {
    pub fn trade_has_usd_data(trade: &TradeRecord) -> bool {
        trade.volume_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
            || trade.price_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
    }

    pub fn trade_get_usd_volume(trade: &TradeRecord) -> Decimal {
        trade.volume_usd.unwrap_or(Decimal::ZERO)
    }

    pub fn wallet_has_usd_data(wallet_tx: &WalletTransaction) -> bool {
        wallet_tx.value_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
    }

    pub fn wallet_get_usd_value(wallet_tx: &WalletTransaction) -> Decimal {
        wallet_tx.value_usd.unwrap_or(Decimal::ZERO)
    }

    pub fn pair_has_usd_data(pair: &TradingPairWithStats) -> bool {
        pair.price_usd > Decimal::ZERO
            || pair.volume_24h_usd > Decimal::ZERO
            || pair.liquidity_usd > Decimal::ZERO
    }

    pub fn pair_get_usd_volume(pair: &TradingPairWithStats) -> Decimal {
        pair.volume_24h_usd
    }

    pub fn pair_get_usd_liquidity(pair: &TradingPairWithStats) -> Decimal {
        pair.liquidity_usd
    }
}
