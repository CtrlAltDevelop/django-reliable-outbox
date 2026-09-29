"""Benchmarks: throughput by worker count, and commit-to-execution latency.

    uv run python bench/run.py                   # everything, default sizes
    uv run python bench/run.py --messages 50000 --workers 1 4 16

Workers are separate processes (``manage.py outbox_worker``), as in production,
so the GIL is not what is being measured. Results are printed and written to
``bench/results/<timestamp>.json`` together with the machine they came from.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bench.settings")

import django  # noqa: E402

django.setup()

from django.core.management import call_command  # noqa: E402
from django.db import connection, transaction  # noqa: E402

from reliable_outbox import publish  # noqa: E402
from reliable_outbox.models import OutboxMessage, Status  # noqa: E402

WORKER_ENV = {**os.environ, "PYTHONPATH": str(ROOT)}


def start_workers(count: int, *args: str) -> list[subprocess.Popen[bytes]]:
    command = [sys.executable, "-m", "django", "outbox_worker", *args]
    return [
        subprocess.Popen(command, cwd=ROOT, env=WORKER_ENV, stderr=subprocess.DEVNULL)
        for _ in range(count)
    ]


def stop_workers(workers: list[subprocess.Popen[bytes]]) -> None:
    for worker in workers:
        worker.terminate()
    for worker in workers:
        worker.wait(timeout=30)


def reset() -> None:
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE reliable_outbox_outboxmessage, bench_latency")


def throughput(workers: int, messages: int, batch_size: int) -> dict[str, Any]:
    """Messages per second from the moment a backlog becomes due to the last ack."""
    reset()
    # The whole backlog becomes due at one instant a few seconds out, so worker
    # start-up and the insert itself are outside the measured window.
    lead = 5.0 + workers * 0.3
    with connection.cursor() as cursor:
        cursor.execute("SELECT now() + make_interval(secs => %s)", [lead])
        (due_at,) = cursor.fetchone()
    for start in range(0, messages, 5_000):
        OutboxMessage.objects.bulk_create(
            OutboxMessage(
                name="bench.noop",
                payload={"n": n},
                destination="default",
                max_attempts=3,
                backoff="exponential",
                run_at=due_at,
            )
            for n in range(start, min(start + 5_000, messages))
        )
    procs = start_workers(
        workers, "--no-notify", "--poll-interval", "0.05", "--batch-size", str(batch_size)
    )
    try:
        while OutboxMessage.objects.filter(status=Status.PENDING).exists():
            time.sleep(0.05)
        # Measured by the database's own clock: first due to last acknowledgement.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT extract(epoch FROM max(completed_at) - %s) "
                "FROM reliable_outbox_outboxmessage",
                [due_at],
            )
            (elapsed,) = cursor.fetchone()
    finally:
        stop_workers(procs)
    elapsed = float(elapsed)
    return {
        "workers": workers,
        "messages": messages,
        "batch_size": batch_size,
        "seconds": round(elapsed, 3),
        "messages_per_second": round(messages / elapsed),
    }


def latency(mode: str, messages: int, poll_interval: float, gap: float) -> dict[str, Any]:
    """Seconds from a publishing transaction's commit to its handler starting."""
    reset()
    flags = ["--poll-interval", str(poll_interval)]
    if mode == "polling":
        flags.append("--no-notify")
    procs = start_workers(1, *flags)
    try:
        # Start measuring only once the worker has booted and handled a message,
        # or its start-up time would be counted as delivery latency.
        publish("bench.noop", {})
        deadline = time.monotonic() + 60
        while OutboxMessage.objects.filter(status=Status.PENDING).exists():
            if time.monotonic() > deadline:
                raise RuntimeError("worker did not start")
            time.sleep(0.05)
        time.sleep(poll_interval * 2)  # and let it go idle
        rng = random.Random(42)
        for _ in range(messages):
            with transaction.atomic():
                publish("bench.latency", {"sent_at": time.time()})
            time.sleep(rng.uniform(gap / 2, gap * 1.5))
        deadline = time.monotonic() + 30
        while (
            OutboxMessage.objects.filter(status=Status.PENDING).exists()
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
    finally:
        stop_workers(procs)
    with connection.cursor() as cursor:
        cursor.execute("SELECT seconds FROM bench_latency ORDER BY seconds")
        samples: list[float] = [float(row[0]) for row in cursor.fetchall()]

    def pct(p: float) -> float:
        return round(samples[min(len(samples) - 1, int(p * len(samples)))] * 1000, 1)

    return {
        "mode": mode,
        "poll_interval": poll_interval,
        "samples": len(samples),
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": round(samples[-1] * 1000, 1),
        "mean_ms": round(statistics.fmean(samples) * 1000, 1),
    }


def machine() -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute("SHOW server_version")
        (server,) = cursor.fetchone()
        cursor.execute("SHOW fsync")
        (fsync,) = cursor.fetchone()
        cursor.execute("SHOW synchronous_commit")
        (sync_commit,) = cursor.fetchone()
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "django": django.get_version(),
        "postgres": server,
        "postgres_fsync": fsync,
        "postgres_synchronous_commit": sync_commit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--messages", type=int, default=20_000)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--latency-messages", type=int, default=200)
    parser.add_argument("--latency-gap", type=float, default=0.05, help="Mean seconds between.")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--skip-latency", action="store_true")
    args = parser.parse_args()

    call_command("migrate", verbosity=0)
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS bench_latency (seconds double precision)")

    results: dict[str, Any] = {"machine": machine(), "throughput": [], "latency": []}
    print(json.dumps(results["machine"], indent=2))
    for workers in args.workers:
        row = throughput(workers, args.messages, args.batch_size)
        results["throughput"].append(row)
        print(f"throughput {row}")
    if not args.skip_latency:
        for mode in ("polling", "notify"):
            row = latency(mode, args.latency_messages, args.poll_interval, args.latency_gap)
            results["latency"].append(row)
            print(f"latency {row}")
    reset()

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "bench" / "results" / f"{stamp}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2) + "\n")
    print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
