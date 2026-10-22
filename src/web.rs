mod cache;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
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

async fn health() -> Response {
    cache::no_store(StatusCode::OK)
}

async fn recent_messages_by_query(
    State(state): State<AppState>,
    Query(query): Query<RecentMessagesQuery>,
    headers: HeaderMap,
) -> Response {
    get_recent_messages(&state, &query.channel, &headers).await
}

async fn recent_messages_by_path(
    State(state): State<AppState>,
    Path(channel): Path<String>,
    headers: HeaderMap,
) -> Response {
    get_recent_messages(&state, &channel, &headers).await
}

async fn get_recent_messages(state: &AppState, channel: &str, headers: &HeaderMap) -> Response {
    let channel = normalize_channel(channel);
    tracing::info!(%channel, "/recent-messages");

    let is_channel_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let is_valid_channel =
        !channel.is_empty() && channel.len() <= 32 && channel.chars().all(is_channel_char);
    if !is_valid_channel {
        tracing::warn!(%channel, "invalid channel");
        return cache::no_store(StatusCode::BAD_REQUEST);
    }

    if !state.store.channels().contains(&channel) {
        let Some(redirect_url) = state.redirect_url.as_deref() else {
            return Json(RecentMessagesResponse {
                messages: Vec::new(),
            })
            .into_response();
        };

        return match fallback_messages::proxy(&state.http, redirect_url, &channel).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%channel, %error, "failed to proxy fallback messages");
                cache::no_store(StatusCode::BAD_GATEWAY)
            }
        };
    }

    let etag = cache::etag(state.store.last_timestamp(&channel).await);
    if cache::matches(headers, &etag) {
        return cache::not_modified(&etag);
    }

    let (messages, last_timestamp_ms) = state.store.recent_messages(&channel).await;
    let etag = cache::etag(last_timestamp_ms);

    let response = Json(RecentMessagesResponse { messages });
    (cache::headers(&etag), response).into_response()
}
