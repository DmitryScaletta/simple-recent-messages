# Specification

The app joins configured Twitch channels and serves their recent chat messages
over HTTP.

## Storage

- Redis enabled
  - Redis is the persistent store.
  - Messages are stored in one sorted set per channel, scored by `tmi-sent-ts`.
  - Each member is a raw IRC line; identical lines collapse into one member.
  - Writes are additive and flushed in batches.
  - A failed flush leaves the batch in a per-channel pending buffer
    - The buffer holds at most `LIMIT` messages, newest kept
    - The next regular flush retries it.
  - Redis is periodically trimmed to `LIMIT` for all configured channels.
  - Pending messages are flushed once more on graceful shutdown.
  - Only one instance normally runs.
    - Two instances overlap only during a CI/CD deployment, while the old instance stops and the new one starts.
    - During that brief overlap, Redis may temporarily exceed `LIMIT` until the next trim.
    - Writes never replace the set, so overlapping instances are safe.
  - Redis may lag memory. A crash can lose messages that were not flushed.
- Redis disabled
  - Memory is the only store.
  - Messages are lost on restart.
- Redis unavailable at startup
  - The service starts and serves from memory.
  - The store retries the connection when the flush loop next tries to persist messages and when the trim loop runs.
  - Messages received while disconnected stay in the pending buffer, capped at `LIMIT` per channel, and are flushed after a connection succeeds.
- Persisted history
  - Loaded only during startup.
  - If Redis was unavailable at startup, history is not reloaded after reconnecting.
- Connection loss
  - The Redis connection manager reconnects automatically.
  - Other Redis errors are logged and do not stop the service.

## Message buffer

- One buffer per configured channel.
- Stores raw IRC lines ordered oldest to newest.
- Messages are identified by `tmi-sent-ts`, assumed unique within a channel.
- Capped at `LIMIT`; oldest messages are evicted first.

## Startup

- Initialization runs before IRC and HTTP start.
- Load the newest `LIMIT` persisted messages per channel using one Redis pipeline.
- Trim Redis to `LIMIT` in the same startup pipeline.
- When `REDIRECT_URL` is set, fetch recent messages for all configured channels in parallel, alongside the Redis load.
- Combine persisted and fallback messages and initialize the buffer.
- Queue the initialized messages for persistence.

## Live messages

- Accept `PRIVMSG` and `USERNOTICE`.
- Add messages to the buffer.
- Queue new messages for persistence.

## Reads

- Configured channel
  - Read from memory.
  - Never use Redis.
- Unconfigured channel
  - Reverse-proxy through `REDIRECT_URL` when set.
  - Otherwise return an empty list.

## Response caching

- Configured-channel responses include:
  - `Cache-Control: public, max-age=5, stale-while-revalidate=5`.
  - Weak `ETag` derived from the newest message's `tmi-sent-ts`, formatted as `W/"<timestamp>"`.
  - The `ETag` is `W/"empty"` when there is no newest timestamp.
- A request whose `If-None-Match` contains the current timestamp `ETag` returns `304 Not Modified` with the same cache headers.
- `*` and `W/"empty"` values are not revalidated; requests carrying only those tags receive `200 OK`.
- Invalid-channel, upstream-failure, and `/api/health` responses use `Cache-Control: no-store`.
