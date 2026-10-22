use std::sync::Arc;

use crate::{fallback_messages, store::MessageStore};

pub async fn backfill(
    store: Arc<MessageStore>,
    redirect_url: Option<String>,
    http: reqwest::Client,
) {
    let channels: Vec<String> = store.channels().iter().cloned().collect();

    for channel in channels {
        let messages =
            match fallback_messages::fetch(&http, redirect_url.as_deref(), &channel).await {
                Ok(Some(messages)) => messages,
                Ok(None) => {
                    tracing::info!("no REDIRECT_URL; skipping backfill");
                    return;
                }
                Err(error) => {
                    tracing::warn!(%channel, %error, "failed to fetch fallback messages");
                    continue;
                }
            };

        let total = messages.len();

        store.startup_backfill(&channel, messages).await;

        tracing::info!(%channel, total, "backfill complete");
    }
}
