use std::{sync::Arc, time::Duration};

use thiserror::Error;
use twitch_irc::{
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
    login::StaticLoginCredentials,
    message::{AsRawIRC, ServerMessage},
};

use crate::store::{Message, MessageStore};

#[derive(Debug, Error)]
pub enum IrcError {
    #[error("failed to join configured Twitch channels: {0}")]
    JoinChannels(#[from] twitch_irc::validate::Error),
}

pub fn spawn(store: Arc<MessageStore>) -> Result<(), IrcError> {
    let (mut incoming_messages, irc_client) =
        TwitchIRCClient::<SecureTCPTransport, StaticLoginCredentials>::new(ClientConfig {
            new_connection_every: Duration::from_millis(550),
            ..ClientConfig::default()
        });

    irc_client.set_wanted_channels(store.channels().clone())?;

    tracing::info!(channels = ?store.channels(), "joining channels");

    tokio::spawn(async move {
        // Keep the client handle alive for as long as the receiver runs.
        let _irc_client = irc_client;

        while let Some(message) = incoming_messages.recv().await {
            let Some((channel, message)) = storeable_message(&message) else {
                continue;
            };

            store.push(channel, message).await;
        }

        tracing::warn!("Twitch IRC message stream ended");
    });

    Ok(())
}

fn storeable_message(message: &ServerMessage) -> Option<(&str, Message)> {
    let (channel, timestamp_ms) = match message {
        ServerMessage::Privmsg(message) => (
            message.channel_login.as_str(),
            message.server_timestamp.timestamp_millis(),
        ),
        ServerMessage::UserNotice(message) => (
            message.channel_login.as_str(),
            message.server_timestamp.timestamp_millis(),
        ),
        _ => return None,
    };

    Some((
        channel,
        Message::from_parts(timestamp_ms, message.source().as_raw_irc()),
    ))
}
