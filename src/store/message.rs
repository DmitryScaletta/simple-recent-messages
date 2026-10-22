/// A Twitch chat message: the raw IRC line plus the fields needed for storage.
#[derive(Clone)]
pub struct Message {
    pub(super) id: String,
    pub(super) timestamp_ms: i64,
    pub(super) raw: String,
}

impl Message {
    pub(crate) fn parse(raw: String) -> Option<Self> {
        let (id, timestamp_ms) = parse_tags(&raw)?;

        Some(Self {
            id: id.to_owned(),
            timestamp_ms,
            raw,
        })
    }

    pub(crate) fn from_parts(id: String, raw: String) -> Self {
        let timestamp_ms = timestamp_ms(&raw).unwrap_or(0);

        Self {
            id,
            timestamp_ms,
            raw,
        }
    }
}

fn parse_tags(raw: &str) -> Option<(&str, i64)> {
    let (tags, _) = raw.strip_prefix('@')?.split_once(' ')?;

    let mut message_id = None;
    let mut timestamp_ms = None;

    for tag in tags.split(';') {
        if let Some(value) = tag.strip_prefix("id=")
            && !value.is_empty()
        {
            message_id = Some(value);
        } else if let Some(value) = tag.strip_prefix("tmi-sent-ts=") {
            timestamp_ms = value.parse().ok();
        }
    }

    Some((message_id?, timestamp_ms?))
}

fn timestamp_ms(raw: &str) -> Option<i64> {
    parse_tags(raw).map(|(_, timestamp_ms)| timestamp_ms)
}
