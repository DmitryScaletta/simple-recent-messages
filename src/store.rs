mod memory;
mod message;
mod redis;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(crate) use self::message::Message;

use self::{memory::MemoryStore, redis::RedisStore};

pub struct MessageStore {
    memory: MemoryStore,
    redis: Option<Arc<RedisStore>>,
}

impl MessageStore {
    pub fn connect(redis_url: Option<&str>, channels: Vec<String>, limit: usize) -> Self {
        Self {
            redis: RedisStore::from_url(redis_url, &channels, limit),
            memory: MemoryStore::new(channels, limit),
        }
    }

    pub fn channels(&self) -> &HashSet<String> {
        self.memory.channels()
    }

    pub async fn push(&self, channel: &str, message: Message) {
        if !self.memory.has_channel(channel) {
            return;
        }
        let added = self.memory.push(channel, message.clone()).await;
        if added && let Some(redis) = &self.redis {
            redis.queue_message(channel, message).await;
        }
    }

    pub async fn load_persisted_messages(&self) -> HashMap<String, Vec<Message>> {
        let Some(redis) = &self.redis else {
            return HashMap::new();
        };
        redis.load_all().await.unwrap_or_default()
    }

    pub async fn init_messages(&self, mut messages_by_channel: HashMap<String, Vec<Message>>) {
        let channels: Vec<String> = self.memory.channels().iter().cloned().collect();
        for channel in channels {
            let messages = messages_by_channel.remove(&channel).unwrap_or_default();
            let messages = self.memory.init(&channel, messages).await;
            if let Some(redis) = &self.redis {
                for message in messages {
                    redis.queue_message(&channel, message).await;
                }
            }
        }
    }

    pub async fn last_timestamp(&self, channel: &str) -> Option<i64> {
        self.memory.last_timestamp(channel).await
    }

    pub async fn recent_messages(&self, channel: &str) -> (Vec<String>, Option<i64>) {
        self.memory.recent_messages(channel).await
    }

    pub async fn flush(&self) {
        if let Some(redis) = &self.redis {
            redis.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const RAW_1: &str = "@id=1;tmi-sent-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m1";
    const RAW_2: &str = "@id=2;tmi-sent-ts=200 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m2";
    const RAW_3: &str = "@id=3;tmi-sent-ts=300 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m3";

    fn message(timestamp_ms: i64, raw: &str) -> Message {
        Message::from_parts(timestamp_ms, raw.to_owned())
    }

    #[tokio::test]
    async fn memory_only_store_works_without_redis() {
        let store = MessageStore::connect(None, vec!["channel".to_owned()], 2);

        store.push("channel", message(100, RAW_1)).await;
        store.push("channel", message(200, RAW_2)).await;
        store.push("channel", message(300, RAW_3)).await;

        let (messages, last_timestamp_ms) = store.recent_messages("channel").await;

        assert_eq!(messages, vec![RAW_2.to_owned(), RAW_3.to_owned()]);
        assert_eq!(last_timestamp_ms, Some(300));
        assert_eq!(store.last_timestamp("channel").await, Some(300));
    }

    #[tokio::test]
    async fn init_messages_sorts_deduplicates_and_caps() {
        let store = MessageStore::connect(None, vec!["channel".to_owned()], 2);

        store
            .init_messages(HashMap::from([(
                "channel".to_owned(),
                vec![
                    message(300, RAW_3),
                    message(100, RAW_1),
                    message(200, RAW_2),
                    message(200, RAW_1),
                ],
            )]))
            .await;

        let (messages, last_timestamp_ms) = store.recent_messages("channel").await;

        assert_eq!(messages, vec![RAW_2.to_owned(), RAW_3.to_owned()]);
        assert_eq!(last_timestamp_ms, Some(300));
    }
}
