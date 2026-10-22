use std::time::Duration;

use axum::{body::Body, response::Response};
use serde::Deserialize;
use thiserror::Error;

#[derive(Deserialize)]
struct RecentMessagesResponse {
    messages: Vec<String>,
}

#[derive(Debug, Error)]
pub enum FetchMessagesError {
    #[error("failed to fetch {url}: {source}")]
    Fetch {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("invalid response from {url}: {source}")]
    Decode {
        url: String,
        #[source]
        source: reqwest::Error,
    },
}

pub async fn fetch(
    http: &reqwest::Client,
    redirect_url: Option<&str>,
    channel: &str,
) -> Result<Option<Vec<String>>, FetchMessagesError> {
    let Some(redirect_url) = redirect_url else {
        return Ok(None);
    };

    let url = redirect_url.replace("%1", channel);
    tracing::info!(%url);

    let response = http
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|source| FetchMessagesError::Fetch {
            url: url.clone(),
            source,
        })?;

    let response: RecentMessagesResponse = response
        .json()
        .await
        .map_err(|source| FetchMessagesError::Decode { url, source })?;

    Ok(Some(response.messages))
}

pub async fn proxy(
    http: &reqwest::Client,
    redirect_url: &str,
    channel: &str,
) -> Result<Response, reqwest::Error> {
    let url = redirect_url.replace("%1", channel);
    tracing::info!(%url);

    let upstream = http
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = upstream.bytes().await?;

    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    *response.headers_mut() = headers;

    Ok(response)
}
