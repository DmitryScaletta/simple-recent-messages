# simple-recent-messages

Minimal Twitch chat buffer. It connects anonymously to Twitch IRC, joins the channels listed in `CHANNELS`, and serves recent raw `PRIVMSG` / `USERNOTICE` lines over HTTP.

- In-memory by default.
- Optional Redis persistence with `REDIS_URL`.
- Startup backfill from `REDIRECT_URL` when set.

See [docs/SPEC.md](docs/SPEC.md) for the main logic.

## Run

```sh
CHANNELS=channel1,channel2 cargo run
# with redis:
REDIS_URL=redis://127.0.0.1:6379 CHANNELS=channel1,channel2 cargo run
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
| `BASE_URL` | no | | Public base URL, e.g. `https://example.com`. |
| `REDIRECT_URL` | no | | Upstream URL template with `%1` replaced by the channel login. |
| `REDIS_URL` | no | | Redis connection URL. Without it, messages are kept only in memory. |
| `RUST_LOG` | no | `info` | Tracing filter. |

`twitch-irc` automatically reconnects and rejoins configured channels.

## Endpoints

```http
GET /api/twitch/recent-messages?channel=xqc
GET /api/twitch/recent-messages/xqc
```

Response: `{"messages": ["<raw IRC line>", ...]}`.

Configured channels are served from memory. Other channels are proxied through `REDIRECT_URL` when it is set, or return an empty list.
