use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::RwLock;

use super::Message;

pub(super) struct MemoryStore {
    channels: HashSet<String>,
    limit: usize,
    buffers: RwLock<HashMap<String, VecDeque<Message>>>,
}

impl MemoryStore {
    pub(super) fn new(channels: Vec<String>, limit: usize) -> Self {
        let channels: HashSet<String> = channels.into_iter().collect();
        let buffers = channels
            .iter()
            .map(|channel| (channel.clone(), VecDeque::with_capacity(limit)))
            .collect();
        Self {
            channels,
            limit,
            buffers: RwLock::new(buffers),
        }
    }

    pub(super) fn channels(&self) -> &HashSet<String> {
        &self.channels
    }

    pub(super) fn has_channel(&self, channel: &str) -> bool {
        self.channels.contains(channel)
    }

    pub(super) async fn push(&self, channel: &str, message: Message) {
        let mut buffers = self.buffers.write().await;
        let Some(buffer) = buffers.get_mut(channel) else {
            return;
        };
        buffer.push_back(message);
        if buffer.len() > self.limit {
            buffer.pop_front();
        }
    }

    pub(super) async fn recent_messages(&self, channel: &str) -> Vec<String> {
        let buffers = self.buffers.read().await;
        buffers
            .get(channel)
            .map(|buffer| buffer.iter().map(|message| message.raw.clone()).collect())
            .unwrap_or_default()
    }

    /// Merges messages into the buffer, then keeps the newest `limit` by timestamp.
    /// Returns the messages that were newly inserted.
    pub(super) async fn merge(
        &self,
        channel: &str,
        messages: impl IntoIterator<Item = Message>,
    ) -> Vec<Message> {
        let mut buffers = self.buffers.write().await;
        let Some(buffer) = buffers.get_mut(channel) else {
            return Vec::new();
        };

        let mut all: Vec<Message> = buffer.drain(..).collect();
        let mut inserted = Vec::new();

        for message in messages {
            if all.iter().any(|existing| existing.id == message.id) {
                continue;
            }
            inserted.push(message.clone());
            all.push(message);
        }

        if inserted.is_empty() {
            buffer.extend(all);
            return inserted;
        }

        all.sort_by_key(|message| message.timestamp_ms);
        let keep_from = all.len().saturating_sub(self.limit);
        buffer.extend(all.into_iter().skip(keep_from));

        inserted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_1: &str = "@id=1;tmi-sent-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #channel :a";
    const RAW_2: &str = "@id=2;tmi-sent-ts=200 :user!user@user.tmi.twitch.tv PRIVMSG #channel :b";
    const RAW_3: &str = "@id=3;tmi-sent-ts=300 :user!user@user.tmi.twitch.tv PRIVMSG #channel :c";

    fn message(id: &str, raw: &str) -> Message {
        Message::from_parts(id.to_owned(), raw.to_owned())
    }

    #[tokio::test]
    async fn caps_buffer() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message("1", RAW_1)).await;
        memory.push("channel", message("2", RAW_2)).await;
        memory.push("channel", message("3", RAW_3)).await;

        assert_eq!(
            memory.recent_messages("channel").await,
            vec![RAW_2.to_owned(), RAW_3.to_owned()]
        );
    }

    #[tokio::test]
    async fn merge_keeps_existing_messages_and_sorts() {
        let memory = MemoryStore::new(vec!["channel".to_owned()], 2);

        memory.push("channel", message("3", RAW_3)).await;

        let inserted = memory
            .merge(
                "channel",
                vec![
                    message("1", RAW_1),
                    message("2", RAW_2),
                    message("3", RAW_3),
                ],
            )
            .await;

        assert_eq!(inserted.len(), 2);
        assert_eq!(
            memory.recent_messages("channel").await,
            vec![RAW_2.to_owned(), RAW_3.to_owned()]
        );
    }
}
