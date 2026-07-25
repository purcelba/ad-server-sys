"""Thread-safe metrics container — the consume loop (background thread)
writes to it, the FastAPI /metrics endpoint (main thread) reads a
snapshot. Matches CLAUDE.md's "every service exposes /health and
/metrics (JSON: request count, error count, latency histogram buckets)"
convention, adapted to a streaming consumer's shape (events consumed,
processing latency, lag, unknown-event counts)."""

from __future__ import annotations

import threading
from collections import defaultdict, deque


class Metrics:
    def __init__(self, latency_window: int = 500):
        self._lock = threading.Lock()
        self._events_consumed_total = 0
        self._events_consumed_by_type: dict[str, int] = defaultdict(int)
        self._unknown_events_by_type: dict[str, int] = defaultdict(int)
        self._latencies_ms: deque[float] = deque(maxlen=latency_window)
        self._lag_seconds = 0.0

    def record_processed(self, event_type: str, latency_ms: float, lag_seconds: float) -> None:
        with self._lock:
            self._events_consumed_total += 1
            self._events_consumed_by_type[event_type] += 1
            self._latencies_ms.append(latency_ms)
            self._lag_seconds = lag_seconds

    def record_unknown(self, event_type: str) -> None:
        with self._lock:
            self._unknown_events_by_type[event_type] += 1

    def snapshot(self) -> dict:
        with self._lock:
            latencies = list(self._latencies_ms)
            avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0.0
            return {
                "events_consumed_total": self._events_consumed_total,
                "events_consumed_by_type": dict(self._events_consumed_by_type),
                "unknown_events_by_type": dict(self._unknown_events_by_type),
                "processing_latency_ms_avg": round(avg_latency_ms, 3),
                "processing_latency_ms_samples": len(latencies),
                "lag_seconds": round(self._lag_seconds, 3),
            }
