use std::{collections::HashMap, sync::Arc};

use tokio::task::JoinSet;

use crate::{
    fallback_messages,
    store::{Message, MessageStore},
};

pub async fn run(store: Arc<MessageStore>, redirect_url: Option<String>, http: reqwest::Client) {
    let messages_by_channel = match redirect_url {
        Some(redirect_url) => {
            let channels: Vec<String> = store.channels().iter().cloned().collect();
            let (mut persisted, fallbacks) = tokio::join!(
                store.load_persisted_messages(),
                fetch_fallbacks(channels, redirect_url, http),
            );

            for (channel, messages) in fallbacks {
                persisted.entry(channel).or_default().extend(messages);
            }

            persisted
        }
        None => {
            tracing::info!("backfill disabled; skipping");
            store.load_persisted_messages().await
        }
    };

    store.init_messages(messages_by_channel).await;
}

async fn fetch_fallbacks(
    channels: Vec<String>,
    redirect_url: String,
    http: reqwest::Client,
) -> HashMap<String, Vec<Message>> {
    let mut fetches = JoinSet::new();
    let mut messages_by_channel: HashMap<String, Vec<Message>> = HashMap::new();

    for channel in channels {
        let http = http.clone();
        let redirect_url = redirect_url.clone();

        fetches.spawn(async move {
            let result =
                fallback_messages::fetch(&http, Some(redirect_url.as_str()), &channel).await;
            (channel, result)
        });
    }

    while let Some(fetched) = fetches.join_next().await {
        let (channel, result) = match fetched {
            Ok(fetched) => fetched,
            Err(error) => {
                tracing::warn!(%error, "failed to fetch fallback messages");
                continue;
            }
        };

        match result {
            Ok(Some(raw_messages)) => {
                tracing::info!(%channel, total = raw_messages.len(), "backfill complete");
                messages_by_channel
                    .entry(channel)
                    .or_default()
                    .extend(raw_messages.into_iter().filter_map(Message::parse));
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%channel, %error, "failed to fetch fallback messages");
            }
        }
    }

    messages_by_channel
}
