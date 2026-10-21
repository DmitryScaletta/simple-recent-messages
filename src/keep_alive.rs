use std::time::Duration;

const PING_INTERVAL: Duration = Duration::from_mins(30);

pub fn spawn(base_url: Option<String>) {
    let Some(base_url) = base_url else {
        tracing::warn!("BASE_URL is not set; health pings are disabled");
        return;
    };

    let health_url = format!("{base_url}/api/health");
    if let Err(error) = reqwest::Url::parse(&health_url) {
        tracing::warn!(%health_url, %error, "invalid BASE_URL; health pings are disabled");
        return;
    }

    tracing::info!(%health_url, "ping every {PING_INTERVAL:?}");

    tokio::spawn(async move {
        let client = reqwest::Client::new();

        loop {
            tokio::time::sleep(PING_INTERVAL).await;

            match client
                .get(&health_url)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(response) => {
                    tracing::info!(%health_url, status = %response.status(), "ping");
                }
                Err(error) => {
                    tracing::warn!(%health_url, %error, "ping");
                }
            }
        }
    });
}
