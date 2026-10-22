mod config;
mod fallback_messages;
mod irc;
mod keep_alive;
mod startup;
mod store;
mod web;

use std::{io, process::ExitCode, sync::Arc};

use thiserror::Error;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use crate::{
    config::{Config, ConfigError},
    irc::IrcError,
    store::MessageStore,
};

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Irc(#[from] IrcError),

    #[error("failed to bind 0.0.0.0:{port}: {source}")]
    Bind {
        port: u16,
        #[source]
        source: io::Error,
    },

    #[error("HTTP server failed: {0}")]
    Serve(#[source] io::Error),
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AppError> {
    #[expect(clippy::unused_result_ok, reason = "the .env file is optional")]
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let http = reqwest::Client::new();
    let store = Arc::new(MessageStore::connect(
        config.redis_url.as_deref(),
        config.channels,
        config.limit,
    ));

    if config.redis_url.is_none() {
        tracing::info!("no REDIS_URL; using in-memory storage only");
    }
    startup::run(
        Arc::clone(&store),
        config.redirect_url.clone(),
        http.clone(),
    )
    .await;
    irc::spawn(Arc::clone(&store))?;

    let app = web::router(
        Arc::clone(&store),
        config.redirect_url.clone(),
        http.clone(),
    );
    let listener = TcpListener::bind(("0.0.0.0", config.port))
        .await
        .map_err(|source| AppError::Bind {
            port: config.port,
            source,
        })?;

    keep_alive::spawn(config.base_url, http);

    tracing::info!(port = config.port, "HTTP server listening");

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    store.flush().await;

    serve_result.map_err(AppError::Serve)?;

    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "failure to install shutdown signal handlers is fatal"
)]
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
