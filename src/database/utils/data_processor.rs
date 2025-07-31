//! 数据处理工具
//!
//! 提供通用的数据处理和批处理功能

use chrono::{DateTime, Utc};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

/// 批处理器
pub struct BatchProcessor {
    batch_size: usize,
}

impl BatchProcessor {
    /// 创建新的批处理器
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }

    /// 使用默认批处理大小创建
    pub fn with_default_size() -> Self {
        Self::new(100)
    }

    /// 批量处理数据
    pub fn process_in_batches<T, R, F>(&self, data: Vec<T>, mut processor: F) -> Vec<R>
    where
        F: FnMut(&[T]) -> Vec<R>,
    {
        let mut results = Vec::new();

        for chunk in data.chunks(self.batch_size) {
            let mut batch_results = processor(chunk);
            results.append(&mut batch_results);
        }

        results
    }

    /// 异步批量处理数据
    pub async fn process_in_batches_async<T, R, F, Fut>(
        &self,
        data: Vec<T>,
        mut processor: F,
    ) -> Vec<R>
    where
        F: FnMut(&[T]) -> Fut,
        Fut: std::future::Future<Output = Vec<R>>,
    {
        let mut results = Vec::new();

        for chunk in data.chunks(self.batch_size) {
            let mut batch_results = processor(chunk).await;
            results.append(&mut batch_results);
        }

        results
    }
}

/// 数据处理器
pub struct DataProcessor;

impl DataProcessor {
    /// 过滤和排序数据
    pub fn filter_and_sort<T, F, G>(mut data: Vec<T>, filter_fn: F, sort_fn: G) -> Vec<T>
    where
        F: Fn(&T) -> bool,
        G: Fn(&T, &T) -> std::cmp::Ordering,
    {
        data.retain(filter_fn);
        data.sort_by(sort_fn);
        data
    }

    /// 分页数据
    pub fn paginate<T: Clone>(
        data: Vec<T>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> (Vec<T>, usize) {
        let total = data.len();
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(total);

        if offset >= total {
            return (vec![], total);
        }

        let end = std::cmp::min(offset + limit, total);
        let paginated_data = data[offset..end].to_vec();

        (paginated_data, total)
    }

    /// 按字段分组数据
    pub fn group_by<T, K, F>(data: Vec<T>, key_fn: F) -> HashMap<K, Vec<T>>
    where
        K: Eq + std::hash::Hash,
        F: Fn(&T) -> K,
    {
        let mut groups = HashMap::new();

        for item in data {
            let key = key_fn(&item);
            groups.entry(key).or_insert_with(Vec::new).push(item);
        }

        groups
    }

    /// 聚合数据
    pub fn aggregate<T, R, F>(data: &[T], aggregator: F) -> Option<R>
    where
        F: Fn(&[T]) -> R,
    {
        if data.is_empty() {
            None
        } else {
            Some(aggregator(data))
        }
    }

    /// 计算移动平均
    pub fn calculate_moving_average(values: &[Decimal], window_size: usize) -> Vec<Decimal> {
        if values.len() < window_size || window_size == 0 {
            return vec![];
        }

        let mut moving_averages = Vec::new();

        for i in window_size - 1..values.len() {
            let window = &values[i + 1 - window_size..=i];
            let sum: Decimal = window.iter().sum();
            let average = sum / Decimal::from(window_size);
            moving_averages.push(average);
        }

        moving_averages
    }

    /// 计算百分位数
    pub fn calculate_percentile(mut values: Vec<Decimal>, percentile: f64) -> Option<Decimal> {
        if values.is_empty() || percentile < 0.0 || percentile > 100.0 {
            return None;
        }

        values.sort();
        let index = (percentile / 100.0) * (values.len() - 1) as f64;
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            Some(values[lower_index])
        } else {
            let lower_value = values[lower_index];
            let upper_value = values[upper_index];
            let weight = Decimal::try_from(index - lower_index as f64).unwrap_or(Decimal::ZERO);
            Some(lower_value + (upper_value - lower_value) * weight)
        }
    }

    /// 检测异常值
    pub fn detect_outliers(values: &[Decimal], threshold: f64) -> Vec<usize> {
        if values.len() < 3 {
            return vec![];
        }

        // 计算四分位数
        let mut sorted_values = values.to_vec();
        sorted_values.sort();

        let q1_index = sorted_values.len() / 4;
        let q3_index = 3 * sorted_values.len() / 4;

        let q1 = sorted_values[q1_index];
        let q3 = sorted_values[q3_index];
        let iqr = q3 - q1;

        let threshold_decimal =
            Decimal::try_from(threshold).unwrap_or(Decimal::from_f64(1.5).unwrap_or(Decimal::ZERO));
        let lower_bound = q1 - iqr * threshold_decimal;
        let upper_bound = q3 + iqr * threshold_decimal;

        values
            .iter()
            .enumerate()
            .filter_map(|(i, &value)| {
                if value < lower_bound || value > upper_bound {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// 数据去重
    pub fn deduplicate<T, K, F>(data: Vec<T>, key_fn: F) -> Vec<T>
    where
        K: Eq + std::hash::Hash,
        F: Fn(&T) -> K,
    {
        let mut seen = std::collections::HashSet::new();
        data.into_iter()
            .filter(|item| seen.insert(key_fn(item)))
            .collect()
    }

    /// 数据验证
    pub fn validate_data<T, F>(data: &[T], validator: F) -> Vec<(usize, String)>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        data.iter()
            .enumerate()
            .filter_map(|(i, item)| match validator(item) {
                Ok(()) => None,
                Err(error) => Some((i, error)),
            })
            .collect()
    }

    /// 时间序列数据重采样
    pub fn resample_timeseries<T>(
        data: Vec<(DateTime<Utc>, T)>,
        interval_minutes: i64,
        aggregator: fn(&[T]) -> T,
    ) -> Vec<(DateTime<Utc>, T)>
    where
        T: Clone,
    {
        if data.is_empty() {
            return vec![];
        }

        let mut resampled = Vec::new();
        let interval = chrono::Duration::minutes(interval_minutes);

        // 按时间间隔分组
        let mut current_bucket_start = data[0].0;
        let mut current_bucket_data = Vec::new();

        for (timestamp, value) in data {
            if timestamp >= current_bucket_start + interval {
                // 处理当前桶
                if !current_bucket_data.is_empty() {
                    let aggregated_value = aggregator(&current_bucket_data);
                    resampled.push((current_bucket_start, aggregated_value));
                    current_bucket_data.clear();
                }

                // 开始新桶
                current_bucket_start = timestamp;
            }

            current_bucket_data.push(value);
        }

        // 处理最后一个桶
        if !current_bucket_data.is_empty() {
            let aggregated_value = aggregator(&current_bucket_data);
            resampled.push((current_bucket_start, aggregated_value));
        }

        resampled
    }

    /// 填充缺失数据
    pub fn fill_missing_data<T>(
        mut data: Vec<(DateTime<Utc>, Option<T>)>,
        fill_strategy: FillStrategy<T>,
    ) -> Vec<(DateTime<Utc>, T)>
    where
        T: Clone,
    {
        let mut filled_data = Vec::new();
        let mut last_valid_value: Option<T> = None;

        for (timestamp, value) in data {
            match value {
                Some(v) => {
                    filled_data.push((timestamp, v.clone()));
                    last_valid_value = Some(v);
                }
                None => {
                    let filled_value = match &fill_strategy {
                        FillStrategy::Forward => last_valid_value.clone(),
                        FillStrategy::Zero(zero_value) => Some(zero_value.clone()),
                        FillStrategy::Skip => None,
                    };

                    if let Some(filled_value) = filled_value {
                        filled_data.push((timestamp, filled_value));
                    }
                }
            }
        }

        filled_data
    }

    /// 计算数据质量指标
    pub fn calculate_data_quality_metrics<T, F>(
        data: &[T],
        validators: Vec<(&str, F)>,
    ) -> DataQualityMetrics
    where
        F: Fn(&T) -> bool,
    {
        let total_records = data.len();
        let mut quality_scores = HashMap::new();

        for (metric_name, validator) in validators {
            let valid_count = data.iter().filter(|item| validator(item)).count();
            let score = if total_records > 0 {
                valid_count as f64 / total_records as f64
            } else {
                0.0
            };
            quality_scores.insert(metric_name.to_string(), score);
        }

        let overall_score = if quality_scores.is_empty() {
            0.0
        } else {
            quality_scores.values().sum::<f64>() / quality_scores.len() as f64
        };

        DataQualityMetrics {
            total_records,
            quality_scores,
            overall_score,
        }
    }
}

/// 填充策略
#[derive(Debug, Clone)]
pub enum FillStrategy<T> {
    /// 前向填充
    Forward,
    /// 使用零值填充
    Zero(T),
    /// 跳过缺失值
    Skip,
}

/// 数据质量指标
#[derive(Debug, Clone)]
pub struct DataQualityMetrics {
    pub total_records: usize,
    pub quality_scores: HashMap<String, f64>,
    pub overall_score: f64,
}

impl DataQualityMetrics {
    /// 获取质量等级
    pub fn get_quality_grade(&self) -> String {
        match self.overall_score {
            score if score >= 0.9 => "A".to_string(),
            score if score >= 0.8 => "B".to_string(),
            score if score >= 0.7 => "C".to_string(),
            score if score >= 0.6 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// 获取最差的质量指标
    pub fn get_worst_metric(&self) -> Option<(String, f64)> {
        self.quality_scores
            .iter()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(name, score)| (name.clone(), *score))
    }
}
