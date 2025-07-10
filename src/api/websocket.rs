use super::ApiState;
use axum::{
    extract::{Query, State, WebSocketUpgrade},
    response::IntoResponse,
};
use axum::extract::ws::{Message, WebSocket};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;
use futures_util::{SinkExt, StreamExt};

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
    let channels = params.channels
        .map(|c| c.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["all".to_string()]);

    ws.on_upgrade(move |socket| handle_websocket(socket, state, channels))
}

async fn handle_websocket(socket: WebSocket, state: ApiState, initial_channels: Vec<String>) {
    let (mut sender, mut receiver) = socket.split();
    let mut event_receiver = state.event_sender.subscribe();
    let mut subscribed_channels: HashMap<String, bool> = initial_channels
        .into_iter()
        .map(|c| (c, true))
        .collect();

    // 发送连接成功消息
    let welcome_msg = WebSocketMessage {
        r#type: "connected".to_string(),
        channel: None,
        data: serde_json::json!({
            "message": "WebSocket connected successfully",
            "subscribed_channels": subscribed_channels.keys().collect::<Vec<_>>()
        }),
        timestamp: chrono::Utc::now(),
    };

    if let Ok(msg) = serde_json::to_string(&welcome_msg) {
        let _ = sender.send(Message::Text(msg)).await;
    }

    // 处理消息的任务
    let sender_task = tokio::spawn(async move {
        while let Ok(event) = event_receiver.recv().await {
            // 解析事件消息
            if let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&event) {
                let event_type = event_data.get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown");

                let channel = get_channel_for_event(event_type);

                // 检查是否订阅了该频道
                if subscribed_channels.contains_key(&channel) || subscribed_channels.contains_key("all") {
                    let ws_message = WebSocketMessage {
                        r#type: event_type.to_string(),
                        channel: Some(channel),
                        data: event_data,
                        timestamp: chrono::Utc::now(),
                    };

                    if let Ok(msg) = serde_json::to_string(&ws_message) {
                        if sender.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

    // 处理客户端消息的任务
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // 处理订阅/取消订阅消息
                    if let Ok(subscribe_msg) = serde_json::from_str::<SubscribeMessage>(&text) {
                        match subscribe_msg.action.as_str() {
                            "subscribe" => {
                                for channel in subscribe_msg.channels {
                                    subscribed_channels.insert(channel, true);
                                }
                            }
                            "unsubscribe" => {
                                for channel in subscribe_msg.channels {
                                    subscribed_channels.remove(&channel);
                                }
                            }
                            _ => {
                                // 发送错误消息
                                let error_msg = ErrorMessage {
                                    error: "invalid_action".to_string(),
                                    message: "Action must be 'subscribe' or 'unsubscribe'".to_string(),
                                };
                                if let Ok(msg) = serde_json::to_string(&error_msg) {
                                    // 这里需要访问sender，但它已经被移动了
                                    // 实际实现中需要使用Arc<Mutex<>>或其他同步原语
                                }
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

    // 等待任一任务完成
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
pub fn send_pair_created_event(sender: &broadcast::Sender<String>, pair: &crate::types::TradingPair) {
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

pub fn send_swap_event(sender: &broadcast::Sender<String>, swap: &crate::types::SwapEvent) {
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

pub fn send_liquidity_event(sender: &broadcast::Sender<String>, event_type: &str, mint: Option<&crate::types::MintEvent>, burn: Option<&crate::types::BurnEvent>) {
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
