use super::ApiState;
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(
    mut socket: axum::extract::ws::WebSocket,
    state: ApiState,
) {
    let mut receiver = state.event_sender.subscribe();

    while let Ok(message) = receiver.recv().await {
        if socket
            .send(axum::extract::ws::Message::Text(message))
            .await
            .is_err()
        {
            break;
        }
    }
}
