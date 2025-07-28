use super::ApiState;
use crate::types::*;
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{Query, State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
pub struct WebSocketQuery {
    pub channels: Option<String>, // 订阅的频道，用逗号分隔
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebSocketMessage {
    pub r#type: String,
    pub channel: Option<String>,
    pub data: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeMessage {
    pub action: String, // "subscribe" or "unsubscribe"
    pub subscriptions: Vec<SubscriptionChannel>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionChannel {
    pub chain_id: i32,
    pub event_type: String, // "swaps", "liquidity", "pairs", "prices"
    pub pair_address: Option<String>, // 可选的交易对地址，为空表示订阅所有交易对
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub error: String,
    pub message: String,
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WebSocketQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    // 解析初始订阅频道
    let initial_subscriptions = parse_initial_channels(params.channels);

    ws.on_upgrade(move |socket| handle_websocket(socket, state, initial_subscriptions))
}

async fn handle_websocket(
    socket: WebSocket,
    state: ApiState,
    initial_subscriptions: Vec<SubscriptionChannel>,
) {
    let (mut sender, mut receiver) = socket.split();
    let mut event_receiver = state.event_sender.subscribe();

    // 使用 Arc<Mutex> 包装订阅状态
    let subscriptions = Arc::new(Mutex::new(
        initial_subscriptions
            .into_iter()
            .map(|sub| (sub, true))
            .collect::<HashMap<SubscriptionChannel, bool>>(),
    ));

    // 发送连接成功消息
    let welcome_msg = WebSocketMessage {
        r#type: "connected".to_string(),
        channel: None,
        data: serde_json::json!({
            "message": "WebSocket connected successfully",
            "subscriptions": subscriptions.lock().await.keys().cloned().collect::<Vec<_>>()
        }),
        timestamp: chrono::Utc::now(),
    };

    if let Ok(msg) = serde_json::to_string(&welcome_msg) {
        let _ = sender.send(Message::Text(msg)).await;
    }

    // 克隆 Arc 用于 sender_task
    let sender_subscriptions = Arc::clone(&subscriptions);
    let sender_task = tokio::spawn(async move {
        while let Ok(event) = event_receiver.recv().await {
            if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event) {
                let event_type = event_data
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown");

                // 跳过未知类型消息
                if event_type == "unknown" {
                    continue;
                }

                // 从事件数据中提取链ID和交易对地址
                let chain_id = event_data
                    .get("data")
                    .and_then(|d| d.get("chain_id"))
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0) as i32;

                let pair_address = event_data
                    .get("data")
                    .and_then(|d| d.get("pair_address"))
                    .and_then(|p| p.as_str())
                    .map(|s| s.to_string());

                // 检查是否有匹配的订阅
                let subs = sender_subscriptions.lock().await;
                let should_send = subs.keys().any(|subscription| {
                    matches_subscription(subscription, event_type, chain_id, &pair_address)
                });

                if should_send {
                    if sender.send(Message::Text(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // 克隆 Arc 用于 receiver_task
    let receiver_subscriptions = Arc::clone(&subscriptions);
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(subscribe_msg) = serde_json::from_str::<SubscribeMessage>(&text) {
                        let mut subs = receiver_subscriptions.lock().await;

                        match subscribe_msg.action.as_str() {
                            "subscribe" => {
                                for subscription in subscribe_msg.subscriptions {
                                    if is_valid_subscription(&subscription) {
                                        subs.insert(subscription, true);
                                    }
                                }
                            }
                            "unsubscribe" => {
                                for subscription in subscribe_msg.subscriptions {
                                    subs.remove(&subscription);
                                }
                            }
                            _ => {
                                // 发送错误消息的逻辑可以在这里实现
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = sender_task => {},
        _ = receiver_task => {},
    }
}

// 解析初始订阅频道
fn parse_initial_channels(channels: Option<String>) -> Vec<SubscriptionChannel> {
    if let Some(channels_str) = channels {
        // 简单解析格式：chain_id:event_type:pair_address
        // 例如：1:swaps:0x123,1:liquidity,2:pairs:0x456
        channels_str
            .split(',')
            .filter_map(|channel| {
                let parts: Vec<&str> = channel.trim().split(':').collect();
                if parts.len() >= 2 {
                    if let Ok(chain_id) = parts[0].parse::<i32>() {
                        let event_type = parts[1].to_string();
                        let pair_address = if parts.len() > 2 && !parts[2].is_empty() {
                            Some(parts[2].to_string())
                        } else {
                            None
                        };

                        return Some(SubscriptionChannel {
                            chain_id,
                            event_type,
                            pair_address,
                        });
                    }
                }
                None
            })
            .collect()
    } else {
        vec![]
    }
}

// 检查事件是否匹配订阅
fn matches_subscription(
    subscription: &SubscriptionChannel,
    event_type: &str,
    chain_id: i32,
    pair_address: &Option<String>,
) -> bool {
    // 检查链ID
    if subscription.chain_id != chain_id {
        return false;
    }

    // 检查事件类型
    let event_category = get_event_category(event_type);
    if subscription.event_type != event_category && subscription.event_type != "all" {
        return false;
    }

    // 检查交易对地址
    match (&subscription.pair_address, pair_address) {
        (Some(sub_pair), Some(event_pair)) => {
            // 订阅了特定交易对，检查是否匹配
            sub_pair.to_lowercase() == event_pair.to_lowercase()
        }
        (None, _) => {
            // 订阅了所有交易对
            true
        }
        (Some(_), None) => {
            // 订阅了特定交易对，但事件没有交易对信息
            false
        }
    }
}

// 获取事件类别
fn get_event_category(event_type: &str) -> String {
    match event_type {
        "new_pair" | "pair_created" => "pairs".to_string(),
        "new_swap" | "swap_event" => "swaps".to_string(),
        "new_mint" | "mint_event" | "new_burn" | "burn_event" => "liquidity".to_string(),
        "price_update" => "prices".to_string(),
        "volume_update" => "volume".to_string(),
        _ => "general".to_string(),
    }
}

// 验证订阅是否有效
fn is_valid_subscription(subscription: &SubscriptionChannel) -> bool {
    // 检查链ID是否有效
    if subscription.chain_id <= 0 {
        return false;
    }

    // 检查事件类型是否有效
    let valid_types = ["swaps", "liquidity", "pairs", "prices", "volume", "all"];
    if !valid_types.contains(&subscription.event_type.as_str()) {
        return false;
    }

    // 检查交易对地址格式（如果提供）
    if let Some(ref pair_address) = subscription.pair_address {
        if pair_address.len() != 42 || !pair_address.starts_with("0x") {
            return false;
        }
    }

    true
}

// 用于发送特定事件的辅助函数
pub fn send_pair_created_event(sender: &broadcast::Sender<String>, pair: &TradingPair) {
    let event = serde_json::json!({
        "type": "new_pair",
        "data": {
            "chain_id": pair.chain_id,
            "address": pair.address,
            "pair_address": pair.address, // 添加pair_address字段用于匹配
            "token0": pair.token0,
            "token1": pair.token1,
            "token0_symbol": pair.token0_symbol,
            "token1_symbol": pair.token1_symbol,
            "block_number": pair.block_number,
            "transaction_hash": pair.transaction_hash
        }
    });
    let _ = sender.send(event.to_string());
}

pub fn send_swap_event(sender: &broadcast::Sender<String>, swap: &SwapEvent) {
    let event = serde_json::json!({
        "type": "new_swap",
        "data": {
            "chain_id": swap.chain_id,
            "pair_address": swap.pair_address,
            "sender": swap.sender,
            "amount0_in": swap.amount0_in,
            "amount1_in": swap.amount1_in,
            "amount0_out": swap.amount0_out,
            "amount1_out": swap.amount1_out,
            "to_address": swap.to_address,
            "block_number": swap.block_number,
            "transaction_hash": swap.transaction_hash,
            "timestamp": swap.timestamp
        }
    });
    let _ = sender.send(event.to_string());
}

pub fn send_liquidity_event(
    sender: &broadcast::Sender<String>,
    event_type: &str,
    mint: Option<&MintEvent>,
    burn: Option<&BurnEvent>,
) {
    let event = if let Some(mint) = mint {
        serde_json::json!({
            "type": "new_mint",
            "data": {
                "chain_id": mint.chain_id,
                "pair_address": mint.pair_address,
                "sender": mint.sender,
                "amount0": mint.amount0,
                "amount1": mint.amount1,
                "block_number": mint.block_number,
                "transaction_hash": mint.transaction_hash,
                "timestamp": mint.timestamp
            }
        })
    } else if let Some(burn) = burn {
        serde_json::json!({
            "type": "new_burn",
            "data": {
                "chain_id": burn.chain_id,
                "pair_address": burn.pair_address,
                "sender": burn.sender,
                "amount0": burn.amount0,
                "amount1": burn.amount1,
                "to_address": burn.to_address,
                "block_number": burn.block_number,
                "transaction_hash": burn.transaction_hash,
                "timestamp": burn.timestamp
            }
        })
    } else {
        return;
    };
    let _ = sender.send(event.to_string());
}
