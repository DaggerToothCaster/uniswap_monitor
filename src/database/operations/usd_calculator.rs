use crate::types::{
    TradingPairWithStats, TradeRecord, WalletTransaction,
};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use sqlx::Row;

/// 统一的USD计算辅助结构
pub struct TradeUsdCalculator;

impl TradeUsdCalculator {
    /// 获取所有锚定币的最新价格
    pub async fn get_anchor_prices(pool: &PgPool) -> Result<HashMap<String, Decimal>, sqlx::Error> {
        let query = r#"
            SELECT DISTINCT ON (UPPER(token_symbol)) 
                UPPER(token_symbol) as symbol,
                price_usd
            FROM token_prices 
            WHERE UPPER(token_symbol) IN ('NOS', 'WKTO')
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

    /// 检查代币是否为锚定币并返回锚定币信息
    fn get_anchor_info(
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
        price_map: &HashMap<String, Decimal>,
    ) -> (Option<String>, Decimal, bool, bool) {
        let anchors = ["NOS", "WKTO"];
        
        let token0_anchor = token0_symbol
            .as_ref()
            .filter(|s| anchors.contains(&s.to_uppercase().as_str()))
            .map(|s| s.to_uppercase());
            
        let token1_anchor = token1_symbol
            .as_ref()
            .filter(|s| anchors.contains(&s.to_uppercase().as_str()))
            .map(|s| s.to_uppercase());

        let anchor = token0_anchor.clone().or(token1_anchor.clone());
        let price = anchor
            .as_ref()
            .and_then(|a| price_map.get(a))
            .cloned()
            .unwrap_or(Decimal::ZERO);

        (anchor, price, token0_anchor.is_some(), token1_anchor.is_some())
    }

    /// 为TradeRecord计算USD字段
    pub async fn calculate_trade_usd_fields(
        pool: &PgPool,
        trades: &mut [TradeRecord],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_anchor_prices(pool).await?;
        
        for trade in trades.iter_mut() {
            let (_, price, token0_is_anchor, token1_is_anchor) = 
                Self::get_anchor_info(&trade.token0_symbol, &trade.token1_symbol, &price_map);

            if price <= Decimal::ZERO {
                trade.volume_usd = Some(Decimal::ZERO);
                trade.price_usd = Some(Decimal::ZERO);
                continue;
            }

            trade.volume_usd = Some(Self::calculate_volume_usd(
                &trade.amount0_in,
                &trade.amount1_in,
                &trade.amount0_out,
                &trade.amount1_out,
                &trade.trade_type,
                price,
                token0_is_anchor,
                token1_is_anchor,
            ));

            trade.price_usd = Some(Self::calculate_price_usd(
                trade.price,
                price,
                token0_is_anchor,
                token1_is_anchor,
            ));
        }
        
        Ok(())
    }

    /// 为WalletTransaction计算USD字段
    pub async fn calculate_wallet_usd_fields(
        pool: &PgPool,
        wallet_txs: &mut [WalletTransaction],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_anchor_prices(pool).await?;
        
        for wallet_tx in wallet_txs.iter_mut() {
            let (_, price, token0_is_anchor, token1_is_anchor) = 
                Self::get_anchor_info(&wallet_tx.token0_symbol, &wallet_tx.token1_symbol, &price_map);

            if price <= Decimal::ZERO {
                wallet_tx.value_usd = Some(Decimal::ZERO);
                wallet_tx.price_usd = Some(Decimal::ZERO);
                continue;
            }

            let usd_value = match (token0_is_anchor, token1_is_anchor) {
                (true, false) => wallet_tx.amount0 * price,
                (false, true) => wallet_tx.amount1 * price,
                (true, true) => wallet_tx.amount0 * price,
                (false, false) => Decimal::ZERO,
            };

            wallet_tx.value_usd = Some(usd_value);
            wallet_tx.price_usd = Some(Self::calculate_price_usd(
                wallet_tx.price.unwrap_or(Decimal::ZERO),
                price,
                token0_is_anchor,
                token1_is_anchor,
            ));
        }
        
        Ok(())
    }

    /// 为TradingPairWithStats计算USD字段
    pub async fn calculate_pair_usd_fields(
        pool: &PgPool,
        pairs: &mut [TradingPairWithStats],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_anchor_prices(pool).await?;
        
        for pair in pairs.iter_mut() {
            let (_, price, token0_is_anchor, token1_is_anchor) = 
                Self::get_anchor_info(&pair.token0_symbol, &pair.token1_symbol, &price_map);

            if price <= Decimal::ZERO {
                continue;
            }

            match (token0_is_anchor, token1_is_anchor) {
                (true, false) => {
                    pair.price_usd = price;
                    pair.volume_24h_usd = pair.volume_24h_token0 * price;
                    pair.liquidity_usd = pair.liquidity_token0 * price * Decimal::from(2);
                }
                (false, true) => {
                    pair.price_usd = if pair.price > Decimal::ZERO {
                        price / pair.price
                    } else {
                        Decimal::ZERO
                    };
                    pair.volume_24h_usd = pair.volume_24h_token1 * price;
                    pair.liquidity_usd = pair.liquidity_token1 * price * Decimal::from(2);
                }
                (true, true) => {
                    pair.price_usd = price;
                    pair.volume_24h_usd = pair.volume_24h_token0 * price;
                    pair.liquidity_usd = pair.liquidity_token0 * price * Decimal::from(2);
                }
                (false, false) => {}
            }
        }
        
        Ok(())
    }

    /// 计算交易量USD
    fn calculate_volume_usd(
        amount0_in: &Decimal,
        amount1_in: &Decimal,
        amount0_out: &Decimal,
        amount1_out: &Decimal,
        trade_type: &str,
        anchor_price: Decimal,
        token0_is_anchor: bool,
        token1_is_anchor: bool,
    ) -> Decimal {
        match (token0_is_anchor, token1_is_anchor) {
            (true, false) => match trade_type {
                "buy" => amount0_in * anchor_price,
                "sell" => amount0_out * anchor_price,
                _ => Decimal::ZERO,
            },
            (false, true) => match trade_type {
                "buy" => amount1_out * anchor_price,
                "sell" => amount1_in * anchor_price,
                _ => Decimal::ZERO,
            },
            (true, true) => match trade_type {
                "buy" => amount0_in * anchor_price,
                "sell" => amount0_out * anchor_price,
                _ => Decimal::ZERO,
            },
            (false, false) => Decimal::ZERO,
        }
    }

    /// 计算价格USD
    fn calculate_price_usd(
        price: Decimal,
        anchor_price: Decimal,
        token0_is_anchor: bool,
        token1_is_anchor: bool,
    ) -> Decimal {
        match (token0_is_anchor, token1_is_anchor) {
            (true, false) => anchor_price,
            (false, true) => {
                if price > Decimal::ZERO {
                    anchor_price / price
                } else {
                    Decimal::ZERO
                }
            }
            (true, true) => anchor_price,
            (false, false) => Decimal::ZERO,
        }
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
    pub fn calculate<T>(items: &[T], get_usd_fn: fn(&T) -> Decimal, has_usd_fn: fn(&T) -> bool) -> Self {
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
