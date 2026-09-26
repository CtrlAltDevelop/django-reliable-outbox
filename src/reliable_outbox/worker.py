"""The worker loop: claim a batch, run each message, record the outcome."""

from __future__ import annotations

import logging
import os
import socket
import threading
import traceback

from django.db import DatabaseError, connections, transaction

from . import queries
from .conf import get_settings
from .destinations import get_destination
from .jobs import get_job
from .models import Kind
from .queries import Claim

logger = logging.getLogger(__name__)

_TRACEBACK_LIMIT = 20_000


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
        stop_event: threading.Event | None = None,
    ) -> None:
        config = get_settings()
        self.name = name or default_worker_name()
        self.alias = using or config.DATABASE
        self.batch_size = batch_size or config.BATCH_SIZE
        self.lease_seconds = lease_seconds or config.LEASE_SECONDS
        self.poll_interval = poll_interval or config.POLL_INTERVAL
        self.stop_event = stop_event or threading.Event()

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
                self.stop_event.wait(self.poll_interval)
        finally:
            connections[self.alias].close()
            logger.info("Worker %s stopped", self.name)

    def run_once(self) -> int:
        """Claim one batch and process it. Returns how many messages were run."""
        batch = queries.claim(
            self.alias, worker=self.name, limit=self.batch_size, lease_seconds=self.lease_seconds
        )
        for claim in batch:
            self.process(claim)
        return len(batch)

    def process(self, claim: Claim) -> None:
        try:
            # The handler's own writes and the acknowledgement share one
            # transaction: either both commit or neither does.
            with transaction.atomic(using=self.alias):
                self._dispatch(claim)
                queries.mark_delivered(self.alias, claim)
        except Exception as exc:
            self._handle_failure(claim, exc)
        else:
            logger.debug("Delivered %s %s", claim.envelope.name, claim.envelope.message_id)

    def _dispatch(self, claim: Claim) -> None:
        if claim.kind == Kind.JOB:
            get_job(claim.envelope.name).run(claim.envelope.payload)
        else:
            get_destination(claim.destination).deliver(claim.envelope)

    def _handle_failure(self, claim: Claim, exc: Exception) -> None:
        envelope = claim.envelope
        dead = claim.attempts >= envelope.max_attempts
        queries.record_failure(
            self.alias,
            claim,
            error=f"{type(exc).__name__}: {exc}",
            traceback="".join(traceback.format_exception(exc))[-_TRACEBACK_LIMIT:],
            dead=dead,
        )
        logger.warning(
            "%s %s failed on attempt %d/%d%s",
            envelope.name,
            envelope.message_id,
            claim.attempts,
            envelope.max_attempts,
            " and is dead" if dead else "",
            exc_info=exc,
        )
