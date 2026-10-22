use std::sync::Arc;

#[derive(Clone)]
pub struct Message {
    pub(super) timestamp_ms: i64,
    pub(super) raw: Arc<str>,
}

impl Message {
    pub(crate) fn parse(raw: String) -> Option<Self> {
        let timestamp_ms = timestamp_ms(&raw)?;

        Some(Self {
            timestamp_ms,
            raw: raw.into(),
        })
    }

    pub(crate) fn from_parts(timestamp_ms: i64, raw: String) -> Self {
        Self {
            timestamp_ms,
            raw: raw.into(),
        }
    }
}

fn timestamp_ms(raw: &str) -> Option<i64> {
    let (tags, _) = raw.strip_prefix('@')?.split_once(' ')?;

    tags.split(';').find_map(|tag| {
        let value = tag.strip_prefix("tmi-sent-ts=")?;
        value.parse().ok()
    })
}
