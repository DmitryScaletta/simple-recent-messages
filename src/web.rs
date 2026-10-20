use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::{config::normalize_channel, store::MessageStore};

#[derive(Debug, Deserialize)]
struct RecentMessagesQuery {
    channel: String,
}

#[derive(Debug, Serialize)]
struct RecentMessagesResponse {
    messages: Vec<String>,
}

pub fn router(store: Arc<MessageStore>) -> Router {
    Router::new()
        .route("/api/twitch/recent-messages", get(recent_messages))
        .with_state(store)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn recent_messages(
    State(store): State<Arc<MessageStore>>,
    Query(query): Query<RecentMessagesQuery>,
) -> Json<RecentMessagesResponse> {
    let channel = normalize_channel(&query.channel);
    tracing::info!(%channel, "/recent-messages");

    Json(RecentMessagesResponse {
        messages: store.recent(&channel).await,
    })
}
