use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::{config::normalize_channel, fallback_messages, store::MessageStore};

#[derive(Clone)]
struct AppState {
    store: Arc<MessageStore>,
    redirect_url: Option<String>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct RecentMessagesQuery {
    channel: String,
}

#[derive(Debug, Serialize)]
struct RecentMessagesResponse {
    messages: Vec<String>,
}

pub fn router(
    store: Arc<MessageStore>,
    redirect_url: Option<String>,
    http: reqwest::Client,
) -> Router {
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
            http,
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

    let is_channel_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let is_valid_channel =
        !channel.is_empty() && channel.len() <= 32 && channel.chars().all(is_channel_char);
    if !is_valid_channel {
        tracing::warn!(%channel, "invalid channel");
        return Err(StatusCode::BAD_REQUEST);
    }

    if state.store.channels().contains(&channel) {
        return Ok(Json(RecentMessagesResponse {
            messages: state.store.recent_messages(&channel).await,
        }));
    }

    let messages = fallback_messages::fetch(&state.http, state.redirect_url.as_deref(), &channel)
        .await
        .map_err(|error| {
            tracing::warn!(%channel, %error, "failed to fetch fallback messages");
            StatusCode::BAD_GATEWAY
        })?
        .unwrap_or_default();

    Ok(Json(RecentMessagesResponse { messages }))
}
