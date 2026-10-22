mod memory;
mod message;
mod redis;

use std::collections::HashSet;

pub(crate) use self::message::Message;

use self::{memory::MemoryStore, redis::RedisStore};

pub struct MessageStore {
    memory: MemoryStore,
    redis: Option<RedisStore>,
}

impl MessageStore {
    pub async fn connect(redis_url: Option<&str>, channels: Vec<String>, limit: usize) -> Self {
        Self {
            redis: RedisStore::connect(redis_url, &channels, limit).await,
            memory: MemoryStore::new(channels, limit),
        }
    }

    pub fn channels(&self) -> &HashSet<String> {
        self.memory.channels()
    }

    pub async fn init_memory_messages_from_redis(&self) {
        let Some(redis) = &self.redis else {
            return;
        };

        let Some(messages_by_channel) = redis.load_all(self.memory.channels()).await else {
            return;
        };

        for (channel, messages) in messages_by_channel {
            self.memory.merge(&channel, messages).await;
        }
    }

    pub async fn push(&self, channel: &str, message: Message) {
        if !self.memory.has_channel(channel) {
            return;
        }

        self.memory.push(channel, message.clone()).await;

        if let Some(redis) = &self.redis {
            redis.queue_message(channel, message);
        }
    }

    pub async fn startup_backfill(&self, channel: &str, raw_messages: Vec<String>) {
        if !self.memory.has_channel(channel) {
            return;
        }

        let messages = raw_messages
            .into_iter()
            .filter_map(Message::parse)
            .collect::<Vec<_>>();

        let inserted = self.memory.merge(channel, messages).await;

        if let Some(redis) = &self.redis {
            for message in inserted {
                redis.queue_message(channel, message);
            }
        }
    }

    pub async fn recent_messages(&self, channel: &str) -> Vec<String> {
        self.memory.recent_messages(channel).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_1: &str = "@id=1;tmi-sent-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m1";
    const RAW_2: &str = "@id=2;tmi-sent-ts=200 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m2";
    const RAW_3: &str = "@id=3;tmi-sent-ts=300 :user!user@user.tmi.twitch.tv PRIVMSG #channel :m3";

    fn message(id: &str, raw: &str) -> Message {
        Message::from_parts(id.to_owned(), raw.to_owned())
    }

    #[tokio::test]
    async fn memory_only_store_works_without_redis() {
        let store = MessageStore::connect(None, vec!["channel".to_owned()], 2).await;

        store.push("channel", message("1", RAW_1)).await;
        store.push("channel", message("2", RAW_2)).await;
        store.push("channel", message("3", RAW_3)).await;

        assert_eq!(
            store.recent_messages("channel").await,
            vec![RAW_2.to_owned(), RAW_3.to_owned()]
        );
    }

    #[tokio::test]
    async fn memory_only_backfill_sorts_and_caps() {
        let store = MessageStore::connect(None, vec!["channel".to_owned()], 2).await;

        store
            .startup_backfill(
                "channel",
                vec![RAW_3.to_owned(), RAW_1.to_owned(), RAW_2.to_owned()],
            )
            .await;

        assert_eq!(
            store.recent_messages("channel").await,
            vec![RAW_2.to_owned(), RAW_3.to_owned()]
        );
    }

    #[tokio::test]
    async fn backfill_keeps_live_messages() {
        let store = MessageStore::connect(None, vec!["channel".to_owned()], 3).await;

        store.push("channel", message("3", RAW_3)).await;
        store
            .startup_backfill("channel", vec![RAW_1.to_owned(), RAW_2.to_owned()])
            .await;

        assert_eq!(
            store.recent_messages("channel").await,
            vec![RAW_1.to_owned(), RAW_2.to_owned(), RAW_3.to_owned()]
        );
    }
}
