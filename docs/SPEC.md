# Specification

The app joins configured Twitch channels and serves their recent chat messages
over HTTP.

## Storage

- Redis enabled
  - Redis is the persistent store.
  - Messages are stored in one sorted set per channel, scored by `tmi-sent-ts`.
  - Each member is a raw IRC line; identical lines collapse into one member.
  - The same message ID with different raw text may exist more than once until trimmed. Memory merges by ID.
  - Writes are additive and flushed in batches.
  - Each instance tracks message counts locally and trims a channel when its estimate exceeds `LIMIT`.
  - Only one instance normally runs. Two instances overlap only during a CI/CD deployment, while the old instance stops and the new one starts.
  - During that brief overlap, Redis may temporarily exceed `LIMIT` because instances do not share their estimates.
  - Writes never replace the set, so overlapping instances are safe.
  - Redis may lag memory. A crash can lose messages that were not flushed.
- Redis disabled
  - Memory is the only store.
  - Messages are lost on restart.

Persistence steps are skipped when Redis is disabled.

Redis connection failure at startup logs a warning and falls back to memory-only.

Other Redis errors are logged and do not stop the service.

## Message buffer

- One buffer per configured channel.
- Stores raw IRC lines.
- Persisted and backfilled messages are merged by Twitch message ID.
- Capped at `LIMIT`; oldest messages are evicted first.

## Startup

- Load persisted messages.
- Start IRC and HTTP.
- Backfill in the background when `REDIRECT_URL` is set.
  - Recover messages missed while offline.
  - Fetch recent messages for each configured channel.
  - Merge with existing messages by ID.
- Sort merged messages and cap them at `LIMIT`.

## Live messages

- Accept `PRIVMSG` and `USERNOTICE`.
- Add messages to the buffer.
- Queue new messages for persistence.

## Reads

- Configured channel
  - Read from memory.
  - Never use Redis.
- Unconfigured channel
  - Proxy through `REDIRECT_URL` when set.
  - Otherwise return an empty list.
