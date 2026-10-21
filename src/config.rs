use std::env;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read CHANNELS: {0}")]
    ChannelsEnv(#[from] env::VarError),

    #[error("CHANNELS must contain at least one channel")]
    EmptyChannels,
}

#[derive(Debug)]
pub struct Config {
    pub channels: Vec<String>,
    pub limit: usize,
    pub port: u16,
    pub base_url: Option<String>,
    pub redirect_url: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let channels = parse_channels(&env::var("CHANNELS")?);

        if channels.is_empty() {
            return Err(ConfigError::EmptyChannels);
        }

        let limit = env::var("LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(500)
            .max(1);

        let port = env::var("PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2790);

        let base_url = env::var("BASE_URL")
            .ok()
            .and_then(|value| normalize_base_url(&value));

        let redirect_url = env::var("REDIRECT_URL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());

        if let Some(url) = redirect_url.as_deref()
            && !url.contains("%1")
        {
            tracing::warn!(%url, "REDIRECT_URL does not contain %1");
        }

        Ok(Self {
            channels,
            limit,
            port,
            base_url,
            redirect_url,
        })
    }
}

fn normalize_base_url(value: &str) -> Option<String> {
    let value = value.trim().trim_end_matches('/');
    if value.is_empty() {
        None
    } else if value.starts_with("http://") || value.starts_with("https://") {
        Some(value.to_owned())
    } else {
        Some(format!("https://{value}"))
    }
}

pub fn parse_channels(input: &str) -> Vec<String> {
    let mut channels = Vec::new();

    for channel in input
        .split(|c: char| c == ',' || c == ';' || c.is_whitespace())
        .map(normalize_channel)
        .filter(|channel| !channel.is_empty())
    {
        if !channels.contains(&channel) {
            channels.push(channel);
        }
    }

    channels
}

pub fn normalize_channel(channel: &str) -> String {
    channel.trim().trim_start_matches('#').to_ascii_lowercase()
}
