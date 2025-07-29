use crate::types::{TradeRecord, TradingPairWithStats, WalletTransaction};
use rust_decimal::Decimal;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::HashMap;

/// 统一的USD计算辅助结构
pub struct TradeUsdCalculator;

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

    /// 为TradeRecord计算USD字段
    pub async fn calculate_trade_usd_fields(
        pool: &PgPool,
        trades: &mut [TradeRecord],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        for trade in trades.iter_mut() {
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
            ));
        }

        Ok(())
    }

    /// 为WalletTransaction计算USD字段 (带调试信息)
    pub async fn calculate_wallet_usd_fields(
        pool: &PgPool,
        wallet_txs: &mut [WalletTransaction],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        if price_map.is_empty() {
            return Ok(());
        }

        for (index, wallet_tx) in wallet_txs.iter_mut().enumerate() {
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
            ));
        }

        Ok(())
    }

    /// 为TradingPairWithStats计算USD字段
    pub async fn calculate_pair_usd_fields(
        pool: &PgPool,
        pairs: &mut [TradingPairWithStats],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;

        for pair in pairs.iter_mut() {
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

    /// 计算token1的USD价格（修正版本）
    /// price 参数表示：1 token1 = price * token0 (即 token1/token0 的比率)
    fn calculate_token1_usd_price(
        price: Decimal,
        token0_price: Option<Decimal>,
        token1_price: Option<Decimal>,
        token0_has_price: bool,
        token1_has_price: bool,
    ) -> Decimal {
        match (token0_has_price, token1_has_price) {
            (true, false) => {
                // 只有token0有USD价格，通过交易价格计算token1的USD价格
                // 如果 1 token1 = price * token0，那么 token1_usd = price * token0_usd
                if price > Decimal::ZERO {
                    let token1_usd_price = price * token0_price.unwrap_or(Decimal::ZERO);

                    token1_usd_price
                } else {
                    Decimal::ZERO
                }
            }
            (false, true) => {
                // 只有token1有USD价格，直接使用
                let token1_usd_price = token1_price.unwrap_or(Decimal::ZERO);
                token1_usd_price
            }
            (true, true) => {
                // 两个代币都有USD价格，优先使用token1的直接价格
                // 但也可以通过token0价格验证一致性
                let direct_price = token1_price.unwrap_or(Decimal::ZERO);
                let calculated_price = if price > Decimal::ZERO {
                    price * token0_price.unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                };

                // 使用直接价格，但可以添加一致性检查
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
