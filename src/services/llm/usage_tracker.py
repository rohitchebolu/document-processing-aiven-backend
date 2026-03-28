"""
Background LLM usage tracker.

Records token usage from LLM calls with near-zero latency impact on the
main pipeline. Events are enqueued via `queue.put_nowait()` and flushed
to the database by a daemon thread in batches.

Context metadata (`extraction_job_id`) flows via
Python contextvars set in the document pipeline. Note that
`concurrent.futures.ThreadPoolExecutor` does not auto-propagate
contextvars, so `pdf_extractor.py` must copy context explicitly when
spawning inner threads.
"""

import atexit
import logging
import queue
import threading
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.services.llm.base import LLMResult

logger = logging.getLogger(__name__)

current_extraction_job_id: ContextVar[Optional[int]] = ContextVar(
    "current_extraction_job_id",
    default=None,
)


@dataclass(frozen=True, slots=True)
class UsageEvent:
    provider: str
    model: str
    call_type: str
    input_tokens: int
    output_tokens: int
    response_time_ms: int
    extraction_job_id: Optional[int]
    created_at: datetime


class UsageTracker:
    """Singleton that records LLM usage events to the database in the background."""

    _FLUSH_INTERVAL = 5.0
    _FLUSH_BATCH_SIZE = 20

    def __init__(self) -> None:
        self._queue: queue.Queue[UsageEvent] = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._drain_loop,
            name="llm-usage-writer",
            daemon=True,
        )
        self._thread.start()
        atexit.register(self._shutdown)

    def record(self, result: LLMResult, call_type: str) -> None:
        """Enqueue a usage event without blocking the request path."""
        event = UsageEvent(
            provider=result.provider,
            model=result.model,
            call_type=call_type,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            response_time_ms=result.response_time_ms,
            extraction_job_id=current_extraction_job_id.get(),
            created_at=datetime.utcnow(),
        )
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            logger.warning("Usage tracker queue full; dropping event")

    def _drain_loop(self) -> None:
        while not self._stop_event.is_set():
            batch = self._collect_batch()
            if batch:
                self._flush(batch)
            self._stop_event.wait(timeout=self._FLUSH_INTERVAL)

        batch = self._collect_batch()
        if batch:
            self._flush(batch)

    def _collect_batch(self) -> list[UsageEvent]:
        batch: list[UsageEvent] = []
        while len(batch) < self._FLUSH_BATCH_SIZE:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _flush(self, batch: list[UsageEvent]) -> None:
        try:
            from src.database import SessionLocal
            from src.models.llm_usage import LLMUsageLog

            db = SessionLocal()
            try:
                for event in batch:
                    db.add(
                        LLMUsageLog(
                            provider=event.provider,
                            model=event.model,
                            call_type=event.call_type,
                            input_tokens=event.input_tokens,
                            output_tokens=event.output_tokens,
                            response_time_ms=event.response_time_ms,
                            extraction_job_id=event.extraction_job_id,
                            created_at=event.created_at,
                        )
                    )
                db.commit()
                logger.debug("Flushed %s LLM usage events", len(batch))
            except Exception:
                db.rollback()
                logger.exception("Failed to flush LLM usage events")
            finally:
                db.close()
        except Exception:
            logger.exception("Failed to create DB session for usage flush")

    def _shutdown(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=3.0)


usage_tracker = UsageTracker()
