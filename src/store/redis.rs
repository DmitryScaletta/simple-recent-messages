use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, mpsc};

use super::Message;

const KEY_PREFIX: &str = env!("CARGO_PKG_NAME");
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const TRIM_INTERVAL: Duration = Duration::from_mins(5);

pub(super) struct RedisStore {
    conn: ConnectionManager,
    tx: mpsc::UnboundedSender<(String, Message)>,
    /// Channel name → (dirty, estimated message count).
    state: Arc<Mutex<HashMap<String, (bool, usize)>>>,
    limit: usize,
}

impl RedisStore {
    pub(super) async fn connect(
        redis_url: Option<&str>,
        channels: &[String],
        limit: usize,
    ) -> Option<Self> {
        let redis_url = redis_url?;

        let client = match redis::Client::open(redis_url) {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "failed to connect to Redis");
                return None;
            }
        };

        let conn = match client.get_connection_manager().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(%error, "failed to connect to Redis");
                return None;
            }
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        let state: Arc<Mutex<HashMap<String, (bool, usize)>>> = Arc::new(Mutex::new(
            channels
                .iter()
                .map(|channel| (channel.clone(), (true, 0)))
                .collect(),
        ));

        let mut flush_conn = conn.clone();
        let flush_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(FLUSH_INTERVAL).await;
                flush(&mut rx, &mut flush_conn, &flush_state, limit).await;
            }
        });
        let mut trim_conn = conn.clone();
        let trim_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TRIM_INTERVAL).await;
                trim(&mut trim_conn, &trim_state, limit).await;
            }
        });

        Some(Self {
            conn,
            tx,
            state,
            limit,
        })
    }

    pub(super) async fn load_all(
        &self,
        channels: &HashSet<String>,
    ) -> Option<Vec<(String, Vec<Message>)>> {
        let mut conn = self.conn.clone();
        let channels: Vec<&String> = channels.iter().collect();

        if channels.is_empty() {
            return Some(Vec::new());
        }

        let mut pipe = redis::pipe();
        for channel in &channels {
            pipe.zrange(channel_key(channel), 0, -1);
        }

        let raws: Vec<Vec<String>> = match pipe.query_async(&mut conn).await {
            Ok(raws) => raws,
            Err(error) => {
                tracing::warn!(%error, "failed to load messages from Redis");
                return None;
            }
        };

        let mut state = self.state.lock().await;
        let messages_by_channel = channels
            .into_iter()
            .zip(raws)
            .map(|(channel, raws)| {
                let channel = channel.to_owned();
                let size = raws.len();
                state.insert(channel.clone(), (size > self.limit, size));
                let messages = raws.into_iter().filter_map(Message::parse).collect();
                (channel, messages)
            })
            .collect();
        drop(state);

        Some(messages_by_channel)
    }

    pub(super) fn queue_message(&self, channel: &str, message: Message) {
        if let Err(error) = self.tx.send((channel.to_owned(), message)) {
            tracing::warn!(%channel, %error, "failed to queue message for Redis");
        }
    }
}

async fn flush(
    rx: &mut mpsc::UnboundedReceiver<(String, Message)>,
    conn: &mut ConnectionManager,
    state: &Mutex<HashMap<String, (bool, usize)>>,
    limit: usize,
) {
    let mut messages_by_channel: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    while let Ok((channel, message)) = rx.try_recv() {
        messages_by_channel
            .entry(channel)
            .or_default()
            .push((message.timestamp_ms, message.raw));
    }

    if messages_by_channel.is_empty() {
        return;
    }

    let mut pipe = redis::pipe();
    let mut channels = Vec::with_capacity(messages_by_channel.len());
    for (channel, items) in &messages_by_channel {
        channels.push(channel.clone());
        pipe.zadd_multiple(channel_key(channel), items);
    }

    let saved: Result<Vec<usize>, redis::RedisError> = pipe.query_async(conn).await;

    let counts = match saved {
        Ok(counts) => counts,
        Err(error) => {
            tracing::warn!(%error, channels = channels.len(), "failed to flush messages to Redis");
            mark_dirty(state, &channels).await;
            return;
        }
    };

    // ZADD returns the number of new members, so updated duplicates are ignored.
    let mut state = state.lock().await;
    for (channel, count) in channels.into_iter().zip(counts) {
        #[expect(clippy::unwrap_used, reason = "seeded in connect")]
        let (dirty, size) = state.get_mut(&channel).unwrap();
        *size = size.saturating_add(count);
        if *size > limit {
            *dirty = true;
        }
    }
}

async fn trim(
    conn: &mut ConnectionManager,
    state: &Mutex<HashMap<String, (bool, usize)>>,
    limit: usize,
) {
    let stop = isize::try_from(limit)
        .unwrap_or(isize::MAX)
        .saturating_add(1)
        .saturating_neg();

    let channels: Vec<String> = {
        let mut state = state.lock().await;
        let mut channels = Vec::new();
        for (channel, (dirty, _)) in state.iter_mut() {
            if *dirty {
                *dirty = false;
                channels.push(channel.clone());
            }
        }
        channels
    };

    if channels.is_empty() {
        return;
    }

    let mut pipe = redis::pipe();
    for channel in &channels {
        pipe.zremrangebyrank(channel_key(channel), 0, stop);
    }

    let trimmed: Result<Vec<usize>, redis::RedisError> = pipe.query_async(conn).await;

    if let Err(error) = trimmed {
        tracing::warn!(%error, channels = channels.len(), "failed to trim messages in Redis");
        mark_dirty(state, &channels).await;
        return;
    }

    let mut state = state.lock().await;
    for channel in &channels {
        #[expect(clippy::unwrap_used, reason = "seeded in connect")]
        let (_, size) = state.get_mut(channel).unwrap();
        *size = limit;
    }
}

async fn mark_dirty(state: &Mutex<HashMap<String, (bool, usize)>>, channels: &[String]) {
    let mut state = state.lock().await;
    for channel in channels {
        #[expect(clippy::unwrap_used, reason = "seeded in connect")]
        let (dirty, _) = state.get_mut(channel).unwrap();
        *dirty = true;
    }
}

fn channel_key(channel: &str) -> String {
    format!("{KEY_PREFIX}:{channel}")
}
