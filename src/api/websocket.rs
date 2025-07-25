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
    pub channels: Vec<String>,
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
    let channels = params
        .channels
        .map(|c| c.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["all".to_string()]);

    ws.on_upgrade(move |socket| handle_websocket(socket, state, channels))
}

async fn handle_websocket(socket: WebSocket, state: ApiState, initial_channels: Vec<String>) {
    let (mut sender, mut receiver) = socket.split();
    let mut event_receiver = state.event_sender.subscribe();

    // 使用 Arc<Mutex> 包装 subscribed_channels 以共享可变状态
    let subscribed_channels = Arc::new(Mutex::new(
        initial_channels
            .into_iter()
            .map(|c| (c, true))
            .collect::<HashMap<String, bool>>(),
    ));

    // 发送连接成功消息
    let welcome_msg = WebSocketMessage {
        r#type: "connected".to_string(),
        channel: None,
        data: serde_json::json!({
            "message": "WebSocket connected successfully",
            "subscribed_channels": subscribed_channels.lock().await.keys().cloned().collect::<Vec<_>>()
        }),
        timestamp: chrono::Utc::now(),
    };

    if let Ok(msg) = serde_json::to_string(&welcome_msg) {
        let _ = sender.send(Message::Text(msg)).await;
    }

    // 克隆 Arc 用于 sender_task
    let sender_channels = Arc::clone(&subscribed_channels);
    let sender_task = tokio::spawn(async move {
        while let Ok(event) = event_receiver.recv().await {
            // 直接使用接收到的字符串，避免重复解析
            if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event) {
                let event_type = event_data
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown");

                // 检查是否是系统自动生成的 unknown 类型消息
                if event_type == "unknown" {
                    continue; // 跳过未知类型消息
                }

                let channel = get_channel_for_event(event_type);

                // 获取锁并检查订阅状态
                let channels = sender_channels.lock().await;
                if channels.contains_key(&channel) || channels.contains_key("all") {
                    // 直接转发原始消息，避免重复包装
                    if sender.send(Message::Text(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // 克隆 Arc 用于 receiver_task
    let receiver_channels = Arc::clone(&subscribed_channels);
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(subscribe_msg) = serde_json::from_str::<SubscribeMessage>(&text) {
                        let mut channels = receiver_channels.lock().await;
                        match subscribe_msg.action.as_str() {
                            "subscribe" => {
                                for channel in subscribe_msg.channels {
                                    channels.insert(channel, true);
                                }
                            }
                            "unsubscribe" => {
                                for channel in subscribe_msg.channels {
                                    channels.remove(&channel);
                                }
                            }
                            _ => {
                                // 注意：这里无法直接发送错误消息，因为 sender 已经被移动
                                // 如果需要发送错误消息，可以考虑其他方式
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

fn get_channel_for_event(event_type: &str) -> String {
    match event_type {
        "new_pair" | "pair_created" => "pairs".to_string(),
        "new_swap" | "swap_event" => "swaps".to_string(),
        "new_mint" | "mint_event" => "liquidity".to_string(),
        "new_burn" | "burn_event" => "liquidity".to_string(),
        "price_update" => "prices".to_string(),
        "volume_update" => "volume".to_string(),
        _ => "general".to_string(),
    }
}

// 用于发送特定事件的辅助函数
pub fn send_pair_created_event(sender: &broadcast::Sender<String>, pair: &TradingPair) {
    let event = serde_json::json!({
        "type": "new_pair",
        "data": {
            "chain_id": pair.chain_id,
            "address": pair.address,
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
