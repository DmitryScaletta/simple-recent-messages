use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::{config::normalize_channel, store::MessageStore};

#[derive(Clone)]
struct AppState {
    store: Arc<MessageStore>,
    redirect_url: Option<String>,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct RecentMessagesQuery {
    channel: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RecentMessagesResponse {
    messages: Vec<String>,
}

pub fn router(store: Arc<MessageStore>, redirect_url: Option<String>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/twitch/recent-messages", get(recent_messages_by_query))
        .route(
            "/api/twitch/recent-messages/{channel}",
            get(recent_messages_by_path),
        )
        .with_state(AppState {
            store,
            redirect_url,
            client: reqwest::Client::new(),
        })
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn recent_messages_by_query(
    State(state): State<AppState>,
    Query(query): Query<RecentMessagesQuery>,
) -> Result<Json<RecentMessagesResponse>, StatusCode> {
    get_recent_messages(&state, &query.channel).await
}

async fn recent_messages_by_path(
    State(state): State<AppState>,
    Path(channel): Path<String>,
) -> Result<Json<RecentMessagesResponse>, StatusCode> {
    get_recent_messages(&state, &channel).await
}

async fn get_recent_messages(
    state: &AppState,
    channel: &str,
) -> Result<Json<RecentMessagesResponse>, StatusCode> {
    let channel = normalize_channel(channel);
    tracing::info!(%channel, "/recent-messages");

    if channel.is_empty() || state.store.channels().contains(&channel) {
        return Ok(Json(RecentMessagesResponse {
            messages: state.store.recent(&channel).await,
        }));
    }

    let Some(redirect_url) = &state.redirect_url else {
        return Ok(Json(RecentMessagesResponse {
            messages: Vec::new(),
        }));
    };

    let url = redirect_url.replace("%1", &channel);
    tracing::info!(%url, "fetching messages from REDIRECT_URL");

    let response = state
        .client
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| {
            tracing::warn!(%url, %error, "failed to fetch messages from REDIRECT_URL");
            StatusCode::BAD_GATEWAY
        })?;

    response.json().await.map(Json).map_err(|error| {
        tracing::warn!(%url, %error, "invalid response from REDIRECT_URL");
        StatusCode::BAD_GATEWAY
    })
}
