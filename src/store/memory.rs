use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::RwLock;

use super::Message;

struct ChannelBuffer {
    messages: VecDeque<Message>,
    timestamps: HashSet<i64>,
}

impl ChannelBuffer {
    fn new(limit: usize) -> Self {
        Self {
            messages: VecDeque::with_capacity(limit),
            timestamps: HashSet::with_capacity(limit),
        }
    }
}

pub(super) struct MemoryStore {
    channels: HashSet<String>,
    limit: usize,
    buffers: HashMap<String, RwLock<ChannelBuffer>>,
}

impl MemoryStore {
    pub(super) fn new(channels: Vec<String>, limit: usize) -> Self {
        let channels: HashSet<String> = channels.into_iter().collect();
        let buffers = channels
            .iter()
            .map(|channel| (channel.clone(), RwLock::new(ChannelBuffer::new(limit))))
            .collect();

        Self {
            channels,
            limit,
            buffers,
        }
    }

    pub(super) fn channels(&self) -> &HashSet<String> {
        &self.channels
    }

    pub(super) async fn push(&self, channel: &str, message: Message) -> bool {
        let Some(buffer) = self.buffers.get(channel) else {
            return false;
        };
        let mut buffer = buffer.write().await;

        if !buffer.timestamps.insert(message.timestamp_ms) {
            return false;
        }

        buffer.messages.push_back(message);
        if buffer.messages.len() > self.limit
            && let Some(oldest) = buffer.messages.pop_front()
        {
            buffer.timestamps.remove(&oldest.timestamp_ms);
        }

        true
    }

    pub(super) async fn init(
        &self,
        channel: &str,
        messages: impl IntoIterator<Item = Message>,
    ) -> Vec<Message> {
        let Some(buffer) = self.buffers.get(channel) else {
            return Vec::new();
        };
        let mut buffer = buffer.write().await;

        let mut messages: Vec<_> = messages.into_iter().collect();
        messages.sort_by_key(|message| message.timestamp_ms);
        messages.dedup_by_key(|message| message.timestamp_ms);

        let excess = messages.len().saturating_sub(self.limit);
        messages.drain(..excess);

        buffer.messages = messages.clone().into();
        buffer.timestamps = messages
            .iter()
            .map(|message| message.timestamp_ms)
            .collect();

        messages
    }

    pub(super) async fn recent_messages(
        &self,
        channel: &str,
        client_last_timestamp: Option<i64>,
    ) -> (Option<Vec<String>>, Option<i64>) {
        let Some(buffer) = self.buffers.get(channel) else {
            return (Some(Vec::new()), None);
        };
        let buffer = buffer.read().await;
        let last_ts = buffer.messages.back().map(|message| message.timestamp_ms);

        if client_last_timestamp.is_some_and(|client_ts| last_ts == Some(client_ts)) {
            return (None, last_ts);
        }

        let messages = buffer
            .messages
            .iter()
            .map(|message| message.raw.to_string())
            .collect();

        (Some(messages), last_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_1: &str = "@id=1;tmi-sent-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #channel :a";
    const RAW_2: &str = "@id=2;tmi-sent-ts=200 :user!user@user.tmi.twitch.tv PRIVMSG #channel :b";
    const RAW_3: &str = "@id=3;tmi-sent-ts=300 :user!user@user.tmi.twitch.tv PRIVMSG #channel :c";

    fn message(timestamp_ms: i64, raw: &str) -> Message {
        Message::from_parts(timestamp_ms, raw.to_owned())
    }

    #[tokio::test]
    async fn caps_buffer() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message(100, RAW_1)).await;
        memory.push("channel", message(200, RAW_2)).await;
        memory.push("channel", message(300, RAW_3)).await;

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", None).await;

        assert_eq!(messages, Some(vec![RAW_2.to_owned(), RAW_3.to_owned()]));
        assert_eq!(last_timestamp_ms, Some(300));
    }

    #[tokio::test]
    async fn deduplicates_by_timestamp() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message(100, RAW_1)).await;
        memory.push("channel", message(100, RAW_2)).await;

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", None).await;

        assert_eq!(messages, Some(vec![RAW_1.to_owned()]));
        assert_eq!(last_timestamp_ms, Some(100));
    }

    #[tokio::test]
    async fn matching_timestamp_returns_no_messages() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message(100, RAW_1)).await;
        memory.push("channel", message(200, RAW_2)).await;

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", Some(200)).await;

        assert!(messages.is_none());
        assert_eq!(last_timestamp_ms, Some(200));
    }

    #[tokio::test]
    async fn older_timestamp_returns_messages() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message(100, RAW_1)).await;
        memory.push("channel", message(200, RAW_2)).await;

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", Some(100)).await;

        assert_eq!(messages, Some(vec![RAW_1.to_owned(), RAW_2.to_owned()]));
        assert_eq!(last_timestamp_ms, Some(200));
    }

    #[tokio::test]
    async fn empty_channel_returns_empty_messages() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", None).await;

        assert_eq!(messages, Some(Vec::new()));
        assert_eq!(last_timestamp_ms, None);
    }

    #[tokio::test]
    async fn init_sorts_deduplicates_and_caps() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        let messages = memory
            .init(
                "channel",
                vec![
                    message(300, RAW_3),
                    message(100, RAW_1),
                    message(200, RAW_2),
                    message(200, RAW_1),
                ],
            )
            .await;

        assert_eq!(
            messages
                .iter()
                .map(|message| message.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![200, 300]
        );

        let (messages, last_timestamp_ms) = memory.recent_messages("channel", None).await;

        assert_eq!(messages, Some(vec![RAW_2.to_owned(), RAW_3.to_owned()]));
        assert_eq!(last_timestamp_ms, Some(300));
    }
}
