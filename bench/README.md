# Benchmarks

```bash
# any PostgreSQL; BENCH_DATABASE_URL defaults to postgres://postgres:postgres@localhost:55436/bench
uv run python bench/run.py                  # 20,000 messages; 1, 4, 16 workers; latency both ways
uv run python bench/run.py --messages 10000 --workers 1 2 4 8 --batch-size 50
```

What it measures:

- **Throughput.** A backlog of no-op `bench.noop` events is inserted with a
  `run_at` a few seconds in the future, N worker *processes* are started and
  left idle, and the clock runs from that `run_at` to the last
  acknowledgement, both read from the database clock. Worker start-up and
  the insert are outside the window. Each message is one claim share, one
  handler call and one committed acknowledgement.
- **Latency.** One worker, then 200 events published one per transaction at
  random intervals averaging 50 ms. The handler records `time.time()` minus
  the `sent_at` the publisher stamped just before committing. Run once with
  `--no-notify` (polling every 1 s) and once with LISTEN/NOTIFY (same 1 s poll
  underneath). Measurement starts only after the worker has handled a warm-up
  message.

Raw results, with the machine details, are written to `bench/results/`.

## Measured

On a 2017 laptop: Intel Core i7-7700HQ (4 cores / 8 threads, 2.8 GHz), 32 GB
RAM, Windows 11. PostgreSQL 16.15 ran in Docker Desktop (WSL 2 backend), with
**default durability settings** (`fsync=on`, `synchronous_commit=on`); a
single durable commit measured about 9 ms there. Python 3.14.7, Django 6.0.8,
psycopg 3.3, workers as separate processes, `BATCH_SIZE=10`. The machine was
also running a desktop session (browser, IDE), so treat these as one honest
data point, not a spec sheet.
Result file: [`results/20260929T164333Z.json`](results/20260929T164333Z.json).

| Workers | Messages | Seconds | Messages/second |
|---:|---:|---:|---:|
| 1 | 10,000 | 96.6 | 104 |
| 4 | 10,000 | 41.3 | 242 |
| 16 | 10,000 | 11.9 | 840 |

| Commit → handler start | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Polling, 1 s interval | 568 ms | 1,014 ms | 1,085 ms | 1,120 ms |
| LISTEN/NOTIFY (1 s poll underneath) | 19 ms | 34 ms | 67 ms | 133 ms |

How to read them:

- **One worker is bound by commit latency.** Every message costs a durable
  commit (the acknowledgement, together with the handler's writes), and on
  this machine that is about 9 ms, so roughly 100 messages a second is the
  ceiling for one sequential worker. That is the price of the handler and the
  ack committing atomically. A server with a battery-backed or NVMe write
  cache commits in well under a millisecond and moves this number a lot.
- **Workers scale because commits overlap.** PostgreSQL groups concurrent
  commits into shared WAL flushes, so 16 workers get about 8× one worker, not
  16×. Beyond that the laptop's 8 threads (shared with Docker and the
  workers' own Python) run out.
- **Run-to-run variance was large.** An earlier run on the same machine
  measured 68 / 233 / 417 messages/second for 1 / 4 / 16 workers. That run's
  NOTIFY latency had a p95 of 3.3 s. The benchmark then slept a fixed 4 s for
  the worker to boot, and the likeliest explanation is that the first messages
  queued behind a slow start-up (a standalone probe of the listener showed
  wake-ups within 5 ms of each NOTIFY), so the warm-up message was added.
  That run's NOTIFY median (30 ms) and its polling numbers matched this one.
- **Polling latency is about half the interval, as it should be**, and its
  maximum is about the interval. NOTIFY cuts the median by ~30× without
  polling any faster; its tail is the time to finish whatever message the
  worker was already on.

## Not measured

There is no Celery comparison here. A fair one needs the same workload,
durable settings on both sides (Redis AOF with `appendfsync always` or
RabbitMQ with publisher confirms and durable queues), and the dual-write step
counted for Celery too, and it hasn't been run. Expect a broker to win on raw
throughput by a wide margin: that is its job, and it doesn't pay for an
atomic acknowledgement in PostgreSQL. To compare on your own hardware, add
a Celery task that does what `bench.noop` does and time the same backlog.
