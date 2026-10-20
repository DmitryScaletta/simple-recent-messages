use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::RwLock;

pub struct MessageStore {
    channels: HashSet<String>,
    limit: usize,
    messages: RwLock<HashMap<String, VecDeque<String>>>,
}

impl MessageStore {
    pub fn new(channels: Vec<String>, limit: usize) -> Self {
        let channels = channels.into_iter().collect::<HashSet<_>>();
        let messages = channels
            .iter()
            .map(|channel| (channel.clone(), VecDeque::new()))
            .collect();

        Self {
            channels,
            limit,
            messages: RwLock::new(messages),
        }
    }

    pub fn channels(&self) -> &HashSet<String> {
        &self.channels
    }

    pub async fn push(&self, channel: &str, message: String) {
        let mut messages = self.messages.write().await;

        let Some(queue) = messages.get_mut(channel) else {
            return;
        };

        if queue.len() >= self.limit {
            queue.pop_front();
        }

        queue.push_back(message);
    }

    pub async fn recent(&self, channel: &str) -> Vec<String> {
        self.messages
            .read()
            .await
            .get(channel)
            .map(|messages| messages.iter().cloned().collect())
            .unwrap_or_default()
    }
}
