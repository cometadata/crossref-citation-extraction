from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Self


def _write_stdout(line: str) -> None:
    print(line, flush=True)


class EventLogger:
    def __init__(
        self,
        *,
        output: Callable[[str], None] = _write_stdout,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._output = output
        self._monotonic = monotonic
        self._now = now

    def emit(self, event: str, **fields: Any) -> None:
        timestamp = self._now().astimezone(UTC).isoformat().replace("+00:00", "Z")
        payload = {"event": event, "timestamp": timestamp, **fields}
        self._output("PIPELINE_EVENT=" + json.dumps(payload, sort_keys=True))

    @contextmanager
    def phase(self, phase: str) -> Iterator[None]:
        started = self._monotonic()
        self.emit("phase_started", phase=phase)
        try:
            yield
        except BaseException as error:
            self.emit(
                "phase_failed",
                phase=phase,
                duration_seconds=round(self._monotonic() - started, 3),
                error_type=type(error).__name__,
            )
            raise
        self.emit(
            "phase_completed",
            phase=phase,
            duration_seconds=round(self._monotonic() - started, 3),
        )


class ProgressTracker:
    def __init__(
        self,
        *,
        phase: str,
        total_bytes: int,
        progress: Callable[[dict[str, object]], None] | None,
        resumed_from_bytes: int = 0,
        interval: float = 60.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._phase = phase
        self._total_bytes = total_bytes
        self._progress = progress
        self._resumed_from_bytes = resumed_from_bytes
        self._interval = interval
        self._monotonic = monotonic
        self._completed = resumed_from_bytes
        self._started = 0.0
        self._last_seen_at = 0.0
        self._last_report_at = 0.0
        self._last_reported_bytes = resumed_from_bytes
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> Self:
        self._started = self._monotonic()
        self._last_seen_at = self._started
        self._last_report_at = self._started
        self._emit(self._started)
        if self._progress is not None and self._interval > 0:
            self._thread = threading.Thread(
                target=self._heartbeat,
                name=f"{self._phase}-progress",
                daemon=True,
            )
            self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        with self._lock:
            completed = self._completed
            last_reported = self._last_reported_bytes
            current = self._last_seen_at
        if completed != last_reported:
            self._emit(current)

    def advance(self, byte_count: int) -> None:
        current = self._monotonic()
        with self._lock:
            self._completed += byte_count
            self._last_seen_at = current
            due = current - self._last_report_at >= self._interval
        if due:
            self._emit(current)

    def _heartbeat(self) -> None:
        while not self._stop.wait(self._interval):
            self._emit(self._monotonic())

    def _emit(self, current: float) -> None:
        if self._progress is None:
            return
        with self._lock:
            completed = self._completed
            elapsed = max(current - self._started, 0.0)
            processed = completed - self._resumed_from_bytes
            rate = processed / elapsed if elapsed > 0 and processed > 0 else None
            remaining = max(self._total_bytes - completed, 0)
            eta = 0.0 if remaining == 0 else remaining / rate if rate else None
            percent = (
                100.0
                if self._total_bytes == 0
                else round(completed * 100 / self._total_bytes, 2)
            )
            payload: dict[str, object] = {
                "phase": self._phase,
                "bytes_completed": completed,
                "bytes_total": self._total_bytes,
                "percent": percent,
                "elapsed_seconds": elapsed,
                "bytes_per_second": rate,
                "eta_seconds": eta,
                "resumed_from_bytes": self._resumed_from_bytes,
            }
            self._last_report_at = current
            self._last_reported_bytes = completed
        self._progress(payload)
