"""The worker loop: claim a batch, run each message, record the outcome."""

from __future__ import annotations

import logging
import os
import random
import socket
import threading
import time
import traceback

from django.db import DatabaseError, connections, transaction
from django.utils import timezone

from . import queries
from .backoff import retry_delay
from .conf import get_settings
from .destinations import get_destination
from .exceptions import PermanentError
from .jobs import get_job
from .models import Kind
from .queries import Claim
from .signals import message_dead_lettered, message_delivered, message_failed
from .waiting import make_waiter

logger = logging.getLogger(__name__)

_TRACEBACK_LIMIT = 20_000


class LeaseLost(Exception):
    """Our lease expired and another worker re-claimed the message mid-run."""


def default_worker_name(index: int = 0) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{index}"[:100]


class Worker:
    """Processes outbox messages until told to stop.

    One ``Worker`` is one sequential consumer with its own database connection.
    Run several (threads or processes) for parallelism; they coordinate through
    row locks alone, so there is no leader and nothing to configure.
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        using: str | None = None,
        batch_size: int | None = None,
        lease_seconds: float | None = None,
        poll_interval: float | None = None,
        notify: bool | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        config = get_settings()
        self.name = name or default_worker_name()
        self.alias = using or config.DATABASE
        self.batch_size = batch_size or config.BATCH_SIZE
        self.lease_seconds = lease_seconds or config.LEASE_SECONDS
        self.poll_interval = poll_interval or config.POLL_INTERVAL
        self.stop_event = stop_event or threading.Event()
        self._waiter = make_waiter(
            self.alias, self.stop_event, notify=config.NOTIFY if notify is None else notify
        )
        self._block_on_dead = config.BLOCK_KEY_ON_DEAD_LETTER
        self._backoff_base = config.BACKOFF_BASE_SECONDS
        self._backoff_cap = config.BACKOFF_MAX_SECONDS
        self._rng = random.Random()  # noqa: S311 - jitter, not cryptography

    def stop(self) -> None:
        self.stop_event.set()

    def run(self, *, burst: bool = False) -> None:
        """Loop until stopped. With ``burst``, return as soon as nothing is due."""
        logger.info("Worker %s started", self.name)
        try:
            while not self.stop_event.is_set():
                try:
                    processed = self.run_once()
                except DatabaseError:
                    # A dropped connection or failover must not kill the worker.
                    logger.exception("Worker %s lost the database; retrying", self.name)
                    connections[self.alias].close()
                    self.stop_event.wait(self.poll_interval)
                    continue
                if processed:
                    continue
                if burst:
                    break
                self._waiter.wait(self.poll_interval)
        finally:
            self._waiter.close()
            connections[self.alias].close()
            logger.info("Worker %s stopped", self.name)

    def run_once(self) -> int:
        """Claim one batch and process it. Returns how many messages were run."""
        # Measured from before the claim, so our idea of the deadline is never
        # later than the database's.
        deadline = time.monotonic() + self.lease_seconds
        batch = queries.claim(
            self.alias,
            worker=self.name,
            limit=self.batch_size,
            lease_seconds=self.lease_seconds,
            block_on_dead=self._block_on_dead,
        )
        for index, claim in enumerate(batch):
            # Don't start a message on a lease that is about to lapse: another
            # worker could claim it mid-run. Give the rest back instead.
            if self.stop_event.is_set() or deadline - time.monotonic() < self.lease_seconds / 2:
                queries.release(self.alias, batch[index:], worker=self.name)
                return index
            self.process(claim)
        return len(batch)

    def process(self, claim: Claim) -> None:
        started = time.monotonic()
        queued_for = (timezone.now() - claim.envelope.created_at).total_seconds()
        try:
            # The handler's own writes and the acknowledgement share one
            # transaction: either both commit or neither does.
            with transaction.atomic(using=self.alias):
                self._dispatch(claim)
                if not queries.mark_delivered(self.alias, claim, worker=self.name):
                    raise LeaseLost
        except LeaseLost:
            # Rolling back undoes this run's database writes; the worker that
            # took over will produce them. Side effects outside the database
            # have happened twice, which is what at-least-once means.
            logger.warning(
                "Lease on %s %s expired mid-run; rolled back",
                claim.envelope.name,
                claim.envelope.message_id,
            )
        except Exception as exc:
            self._handle_failure(claim, exc)
        else:
            logger.debug("Delivered %s %s", claim.envelope.name, claim.envelope.message_id)
            message_delivered.send_robust(
                sender=type(self),
                envelope=claim.envelope,
                duration=time.monotonic() - started,
                queued_for=max(queued_for, 0.0),
            )

    def _dispatch(self, claim: Claim) -> None:
        if claim.kind == Kind.JOB:
            get_job(claim.envelope.name).run(claim.envelope.payload)
        else:
            get_destination(claim.destination).deliver(claim.envelope)

    def _handle_failure(self, claim: Claim, exc: Exception) -> None:
        envelope = claim.envelope
        error = f"{type(exc).__name__}: {exc}"
        trace = "".join(traceback.format_exception(exc))[-_TRACEBACK_LIMIT:]
        if isinstance(exc, PermanentError) or claim.attempts >= envelope.max_attempts:
            recorded = queries.mark_dead(
                self.alias, claim, worker=self.name, error=error, traceback=trace
            )
            outcome = "dead-lettered"
            retry_in = None
        else:
            delay = retry_delay(
                claim.backoff,
                claim.attempts,
                base=self._backoff_base,
                cap=self._backoff_cap,
                rng=self._rng,
            )
            recorded = queries.schedule_retry(
                self.alias, claim, worker=self.name, delay=delay, error=error, traceback=trace
            )
            outcome = f"retrying in {delay:.2f}s"
            retry_in = delay
        if not recorded:
            logger.warning("Lease on %s expired before its failure was recorded", envelope.name)
            return
        logger.warning(
            "%s %s failed on attempt %d/%d, %s",
            envelope.name,
            envelope.message_id,
            claim.attempts,
            envelope.max_attempts,
            outcome,
            exc_info=exc,
        )
        if retry_in is None:
            message_dead_lettered.send_robust(sender=type(self), envelope=envelope, error=exc)
        else:
            message_failed.send_robust(
                sender=type(self), envelope=envelope, error=exc, retry_in=retry_in
            )
