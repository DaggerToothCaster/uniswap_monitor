use crate::types::{KLineData, TradeRecord, TradingPairWithStats, WalletTransaction, TimeSeriesData};
use rust_decimal::Decimal;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug)]
pub struct TradeUsdStats {
    pub total_trades: usize,
    pub trades_with_usd_data: usize,
    pub total_volume_usd: Decimal,
    pub avg_trade_size_usd: Decimal,
}

#[derive(Debug)]
pub struct WalletUsdStats {
    pub total_transactions: usize,
    pub transactions_with_usd_data: usize,
    pub total_volume_usd: Decimal,
    pub avg_transaction_size_usd: Decimal,
}

#[derive(Debug)]
pub struct PairUsdStats {
    pub total_pairs: usize,
    pub pairs_with_usd_data: usize,
    pub total_volume_24h_usd: Decimal,
    pub total_liquidity_usd: Decimal,
}

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

    /// 为WalletTransaction计算USD字段
    pub async fn calculate_wallet_usd_fields(
        pool: &PgPool,
        wallet_txs: &mut [WalletTransaction],
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;
        
        if price_map.is_empty() {
            return Ok(());
        }

        for wallet_tx in wallet_txs.iter_mut() {
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

    /// 为K线数据计算USD字段 - 统一使用get_token_prices
    pub async fn calculate_kline_usd_fields(
        pool: &PgPool,
        klines: &mut [KLineData],
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;
        
        let (token0_price, token0_has_price) =
            Self::get_token_price_info(token0_symbol, &price_map);
        let (token1_price, token1_has_price) =
            Self::get_token_price_info(token1_symbol, &price_map);

        // 如果都没有价格信息，无法转换
        if !token0_has_price && !token1_has_price {
            return Ok(());
        }

        for kline in klines.iter_mut() {
            // 转换所有价格字段为USD
            kline.open = Self::calculate_token1_usd_price(
                kline.open,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
            
            kline.high = Self::calculate_token1_usd_price(
                kline.high,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
            
            kline.low = Self::calculate_token1_usd_price(
                kline.low,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
            
            kline.close = Self::calculate_token1_usd_price(
                kline.close,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
        }
        
        Ok(())
    }

    /// 为时间序列数据计算USD字段 - 统一使用get_token_prices
    pub async fn calculate_timeseries_usd_fields(
        pool: &PgPool,
        timeseries: &mut [TimeSeriesData],
        token0_symbol: &Option<String>,
        token1_symbol: &Option<String>,
    ) -> Result<(), sqlx::Error> {
        let price_map = Self::get_token_prices(pool).await?;
        
        let (token0_price, token0_has_price) =
            Self::get_token_price_info(token0_symbol, &price_map);
        let (token1_price, token1_has_price) =
            Self::get_token_price_info(token1_symbol, &price_map);

        // 如果都没有价格信息，无法转换
        if !token0_has_price && !token1_has_price {
            return Ok(());
        }

        for data in timeseries.iter_mut() {
            data.price = Self::calculate_token1_usd_price(
                data.price,
                token0_price,
                token1_price,
                token0_has_price,
                token1_has_price,
            );
        }
        
        Ok(())
    }

    /// 计算token1的USD价格（统一的价格转换逻辑）
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

    // ==================== 数据筛选和排序 ====================

    /// 通用筛选函数
    pub fn filter_with_usd_data<T>(items: Vec<T>, has_usd_fn: fn(&T) -> bool) -> Vec<T> {
        items.into_iter().filter(has_usd_fn).collect()
    }

    /// 通用排序函数
    pub fn sort_by_usd_value<T>(items: &mut [T], get_usd_fn: fn(&T) -> Decimal) {
        items.sort_by(|a, b| get_usd_fn(b).cmp(&get_usd_fn(a)));
    }

    /// 筛选有USD数据的交易记录
    pub fn filter_trades_with_usd_data(trades: Vec<TradeRecord>) -> Vec<TradeRecord> {
        Self::filter_with_usd_data(trades, Self::trade_has_usd_data)
    }

    /// 筛选有USD数据的钱包交易
    pub fn filter_wallet_txs_with_usd_data(
        wallet_txs: Vec<WalletTransaction>,
    ) -> Vec<WalletTransaction> {
        Self::filter_with_usd_data(wallet_txs, Self::wallet_has_usd_data)
    }

    /// 筛选有USD数据的交易对
    pub fn filter_pairs_with_usd_data(
        pairs: Vec<TradingPairWithStats>,
    ) -> Vec<TradingPairWithStats> {
        Self::filter_with_usd_data(pairs, Self::pair_has_usd_data)
    }

    /// 按USD交易量排序交易记录
    pub fn sort_trades_by_usd_volume(trades: &mut [TradeRecord]) {
        Self::sort_by_usd_value(trades, Self::trade_get_usd_volume);
    }

    /// 按USD交易量排序钱包交易
    pub fn sort_wallet_txs_by_usd_value(wallet_txs: &mut [WalletTransaction]) {
        Self::sort_by_usd_value(wallet_txs, Self::wallet_get_usd_value);
    }

    /// 按USD交易量排序交易对
    pub fn sort_pairs_by_usd_volume(pairs: &mut [TradingPairWithStats]) {
        Self::sort_by_usd_value(pairs, Self::pair_get_usd_volume);
    }

    /// 按USD流动性排序交易对
    pub fn sort_pairs_by_usd_liquidity(pairs: &mut [TradingPairWithStats]) {
        pairs.sort_by(|a, b| b.liquidity_usd.cmp(&a.liquidity_usd));
    }

    // ==================== 统计功能 ====================

    /// 获取交易USD统计
    pub fn get_trade_usd_stats(trades: &[TradeRecord]) -> TradeUsdStats {
        let trades_with_usd = trades
            .iter()
            .filter(|trade| Self::trade_has_usd_data(trade))
            .collect::<Vec<_>>();

        let total_volume_usd = trades_with_usd
            .iter()
            .map(|trade| Self::trade_get_usd_volume(trade))
            .sum();

        let avg_trade_size_usd = if !trades_with_usd.is_empty() {
            total_volume_usd / Decimal::from(trades_with_usd.len())
        } else {
            Decimal::ZERO
        };

        TradeUsdStats {
            total_trades: trades.len(),
            trades_with_usd_data: trades_with_usd.len(),
            total_volume_usd,
            avg_trade_size_usd,
        }
    }

    /// 获取钱包交易USD统计
    pub fn get_wallet_usd_stats(wallet_txs: &[WalletTransaction]) -> WalletUsdStats {
        let txs_with_usd = wallet_txs
            .iter()
            .filter(|tx| Self::wallet_has_usd_data(tx))
            .collect::<Vec<_>>();

        let total_volume_usd = txs_with_usd
            .iter()
            .map(|tx| Self::wallet_get_usd_value(tx))
            .sum();

        let avg_tx_size_usd = if !txs_with_usd.is_empty() {
            total_volume_usd / Decimal::from(txs_with_usd.len())
        } else {
            Decimal::ZERO
        };

        WalletUsdStats {
            total_transactions: wallet_txs.len(),
            transactions_with_usd_data: txs_with_usd.len(),
            total_volume_usd,
            avg_transaction_size_usd: avg_tx_size_usd,
        }
    }

    /// 获取交易对USD统计
    pub fn get_pair_usd_stats(pairs: &[TradingPairWithStats]) -> PairUsdStats {
        let pairs_with_usd = pairs
            .iter()
            .filter(|pair| Self::pair_has_usd_data(pair))
            .collect::<Vec<_>>();

        let total_volume_usd = pairs_with_usd
            .iter()
            .map(|pair| pair.volume_24h_usd)
            .sum();

        let total_liquidity_usd = pairs_with_usd
            .iter()
            .map(|pair| pair.liquidity_usd)
            .sum();

        PairUsdStats {
            total_pairs: pairs.len(),
            pairs_with_usd_data: pairs_with_usd.len(),
            total_volume_24h_usd: total_volume_usd,
            total_liquidity_usd,
        }
    }

    // ==================== 辅助函数 ====================

    /// 检查交易记录是否有USD数据
    pub fn trade_has_usd_data(trade: &TradeRecord) -> bool {
        trade.volume_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
            || trade.price_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
    }

    /// 获取交易记录的USD交易量
    pub fn trade_get_usd_volume(trade: &TradeRecord) -> Decimal {
        trade.volume_usd.unwrap_or(Decimal::ZERO)
    }

    /// 检查钱包交易是否有USD数据
    pub fn wallet_has_usd_data(wallet_tx: &WalletTransaction) -> bool {
        wallet_tx.value_usd.unwrap_or(Decimal::ZERO) > Decimal::ZERO
    }

    /// 获取钱包交易的USD价值
    pub fn wallet_get_usd_value(wallet_tx: &WalletTransaction) -> Decimal {
        wallet_tx.value_usd.unwrap_or(Decimal::ZERO)
    }

    /// 检查交易对是否有USD数据
    pub fn pair_has_usd_data(pair: &TradingPairWithStats) -> bool {
        pair.price_usd > Decimal::ZERO
            || pair.volume_24h_usd > Decimal::ZERO
            || pair.liquidity_usd > Decimal::ZERO
    }

    /// 获取交易对的USD交易量
    pub fn pair_get_usd_volume(pair: &TradingPairWithStats) -> Decimal {
        pair.volume_24h_usd
    }

    /// 获取交易对的USD流动性
    pub fn pair_get_usd_liquidity(pair: &TradingPairWithStats) -> Decimal {
        pair.liquidity_usd
    }
}

/// 通用USD统计信息
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
