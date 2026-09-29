# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-29

### Added

- `publish(event_type, payload, key=...)` writes an event in the caller's
  transaction; a rollback publishes nothing. `REQUIRE_TRANSACTION` refuses
  autocommit use.
- `@job(retries=..., backoff=...)` with `.enqueue()` and `.enqueue_with()`
  (key, `run_at`, `delay`), sharing the outbox's guarantees.
- `manage.py outbox_worker`: `FOR UPDATE SKIP LOCKED` claims, fenced leases
  (`locked_until`) so a dead worker's messages return, `--concurrency` threads,
  `--burst` mode, graceful shutdown on SIGTERM/SIGINT/SIGBREAK.
- Handlers and the acknowledgement share one transaction, so a handler's own
  database writes commit exactly when the message is marked delivered.
- Retries with exponential backoff and full jitter, or a fixed delay.
- Dead-letter state with the last error and traceback; `PermanentError` to skip
  remaining attempts; `outbox_requeue` command and admin action.
- Per-key ordering across workers, with `BLOCK_KEY_ON_DEAD_LETTER` to choose
  between strict order and letting a key move past a dead message.
- `run_at` / `delay` scheduling, measured on the database clock.
- `LISTEN/NOTIFY` wake-ups from a statement-level trigger, polling underneath.
- Destinations behind a `Destination` protocol: local `@handler` registry,
  Redis Streams (`redis` extra), Kafka (`kafka` extra), HMAC-SHA256 signed
  webhooks with `verify_signature` for receivers.
- Inbox pattern: `inbox.claim()` and `@inbox.idempotent()` for consumer-side
  deduplication by `message_id`.
- Django admin with state filters (ready, in flight, scheduled, retrying),
  search and requeue.
- `outbox_cleanup` (batched deletes of delivered, optionally dead and inbox
  rows) and `outbox_stats`; `message_delivered`, `message_failed` and
  `message_dead_lettered` signals for metrics.
- Typed, validated settings under `RELIABLE_OUTBOX`; `py.typed`.

[Unreleased]: https://github.com/CtrlAltDevelop/django-reliable-outbox/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/CtrlAltDevelop/django-reliable-outbox/releases/tag/v0.1.0
