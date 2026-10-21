# simple-recent-messages

Minimal in-memory Twitch chat buffer. It connects anonymously to Twitch IRC,
joins the channels listed in `CHANNELS`, and serves the latest raw
`PRIVMSG` / `USERNOTICE` lines over HTTP.

No database is used. Messages are lost when the process restarts.

## Run

```sh
CHANNELS=channel1,channel2 cargo run
```

Or copy `.env.example` to `.env`:

```sh
cp .env.example .env
cargo run
```

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `CHANNELS` | yes | | Comma or semicolon separated channel logins. Leading `#` is allowed. |
| `LIMIT` | no | `500` | Maximum messages kept per channel. |
| `PORT` | no | `2790` | HTTP listen port. |
| `BASE_URL` | no | | Public base URL, e.g. `https://example.com` |
| `REDIRECT_URL` | no | | Upstream URL template with `%1` replaced by the channel login. |
| `RUST_LOG` | no | `info` | Tracing filter. |

Configured channels are joined at startup. `twitch-irc` automatically reconnects
and rejoins them if the connection drops.

## Endpoints

```http
GET /api/twitch/recent-messages?channel=xqc
GET /api/twitch/recent-messages/xqc
```

`channel` is matched case-insensitively and must be a valid Twitch login. Channels that are in `CHANNELS` are served from the in-memory buffer.

Channels that are not in `CHANNELS` return an empty message list by default.
When `REDIRECT_URL` is set, their messages are fetched from that URL with `%1`
replaced by the channel login.

Example response:

```json
{
  "messages": [
    "@badge-info=;badges=;color=#1E90FF;display-name=User;... PRIVMSG #foo :hello"
  ]
}
```

The raw IRC lines include Twitch tags.
