use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, OnceCell};

use super::Message;

const KEY_PREFIX: &str = env!("CARGO_PKG_NAME");
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const TRIM_INTERVAL: Duration = Duration::from_mins(5);

type PendingQueues = HashMap<String, VecDeque<(i64, String)>>;

pub(super) struct RedisStore {
    client: redis::Client,
    conn: OnceCell<ConnectionManager>,
    pending: Mutex<PendingQueues>,
    flush_lock: Mutex<()>,
    channels: Vec<String>,
    limit: usize,
}

impl RedisStore {
    pub(super) fn from_url(
        redis_url: Option<&str>,
        channels: &[String],
        limit: usize,
    ) -> Option<Arc<Self>> {
        let redis_url = redis_url?;

        let client = match redis::Client::open(redis_url) {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "invalid REDIS_URL");
                return None;
            }
        };

        let store = Arc::new(Self {
            client,
            conn: OnceCell::new(),
            pending: Mutex::new(HashMap::new()),
            flush_lock: Mutex::new(()),
            channels: channels.to_vec(),
            limit,
        });

        let flush_store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(FLUSH_INTERVAL).await;
                flush_store.flush().await;
            }
        });
        let trim_store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TRIM_INTERVAL).await;
                trim_store.trim().await;
            }
        });

        Some(store)
    }

    /// Returns a live connection, connecting lazily and retrying after failures.
    async fn connection(&self) -> Option<ConnectionManager> {
        let conn = self
            .conn
            .get_or_try_init(|| async {
                match self.client.get_connection_manager().await {
                    Ok(conn) => {
                        tracing::info!("connected to Redis");
                        Ok(conn)
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to connect to Redis; will retry");
                        Err(error)
                    }
                }
            })
            .await
            .ok()?;

        Some(conn.clone())
    }

    pub(super) async fn load_all(&self) -> Option<HashMap<String, Vec<Message>>> {
        if self.channels.is_empty() {
            return Some(HashMap::new());
        }

        let mut conn = self.connection().await?;

        let mut pipe = redis::pipe();
        for channel in &self.channels {
            // Trim everything before the newest LIMIT entries.
            pipe.zremrangebyrank(channel_key(channel), 0, oldest_stop(self.limit))
                .ignore();
            // Negative ranks select the newest entries: -LIMIT through -1.
            pipe.zrange(channel_key(channel), newest_start(self.limit), -1);
        }

        let raws: Vec<Vec<String>> = match pipe.query_async(&mut conn).await {
            Ok(raws) => raws,
            Err(error) => {
                tracing::warn!(%error, "failed to load messages from Redis");
                return None;
            }
        };

        let messages_by_channel: HashMap<String, Vec<Message>> = self
            .channels
            .iter()
            .cloned()
            .zip(raws)
            .map(|(channel, raws)| {
                let messages = raws.into_iter().filter_map(Message::parse).collect();
                (channel, messages)
            })
            .collect();

        Some(messages_by_channel)
    }

    pub(super) async fn queue_message(&self, channel: &str, message: Message) {
        let mut pending_guard = self.pending.lock().await;
        let queue = pending_guard.entry(channel.to_owned()).or_default();
        queue.push_back((message.timestamp_ms, message.raw));
        if queue.len() > self.limit {
            queue.pop_front();
        }
    }

    pub(super) async fn flush(&self) {
        let _flush_guard = self.flush_lock.lock().await;

        let mut pending_queues = self.take_pending_queues().await;

        if pending_queues.is_empty() {
            return;
        }

        let Some(mut conn) = self.connection().await else {
            self.restore_pending(pending_queues).await;
            return;
        };

        let mut pipe = redis::pipe();
        for (channel, queue) in &mut pending_queues {
            pipe.zadd_multiple(channel_key(channel), queue.make_contiguous());
        }

        if let Err(error) = pipe.exec_async(&mut conn).await {
            tracing::warn!(%error, "failed to flush messages to Redis");
            self.restore_pending(pending_queues).await;
        }
    }

    async fn take_pending_queues(&self) -> PendingQueues {
        let mut pending_guard = self.pending.lock().await;
        std::mem::take(&mut *pending_guard)
    }

    async fn restore_pending(&self, failed_queues: PendingQueues) {
        let mut pending_guard = self.pending.lock().await;
        for (channel, mut older_messages) in failed_queues {
            let newer_messages = pending_guard.entry(channel).or_default();
            older_messages.append(newer_messages);
            while older_messages.len() > self.limit {
                older_messages.pop_front();
            }
            *newer_messages = older_messages;
        }
    }

    async fn trim(&self) {
        if self.channels.is_empty() {
            return;
        }

        let Some(mut conn) = self.connection().await else {
            return;
        };

        let mut pipe = redis::pipe();
        for channel in &self.channels {
            pipe.zremrangebyrank(channel_key(channel), 0, oldest_stop(self.limit));
        }

        if let Err(error) = pipe.exec_async(&mut conn).await {
            tracing::warn!(%error, channels = self.channels.len(), "failed to trim messages in Redis");
        }
    }
}

fn newest_start(limit: usize) -> isize {
    isize::try_from(limit)
        .unwrap_or(isize::MAX)
        .saturating_neg()
}

fn oldest_stop(limit: usize) -> isize {
    isize::try_from(limit)
        .unwrap_or(isize::MAX)
        .saturating_add(1)
        .saturating_neg()
}

fn channel_key(channel: &str) -> String {
    format!("{KEY_PREFIX}:{channel}")
}
