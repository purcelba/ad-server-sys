"""Thread-safe metrics: request count, error count, latency histogram
buckets, p99 — matches CLAUDE.md's global /metrics convention. Same
shape as stream_features/metrics.py, adapted from "events" to "requests".
"""

from __future__ import annotations

import threading
from collections import deque

# upper bounds in ms; a request's latency is counted into every bucket
# it's <= (cumulative), so /metrics reads like a standard latency histogram
LATENCY_BUCKETS_MS = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1000]


class Metrics:
    def __init__(self, latency_window: int = 1000):
        self._lock = threading.Lock()
        self._request_count = 0
        self._error_count = 0
        self._latencies_ms: deque[float] = deque(maxlen=latency_window)

    def record_request(self, latency_ms: float, error: bool = False) -> None:
        with self._lock:
            self._request_count += 1
            if error:
                self._error_count += 1
            self._latencies_ms.append(latency_ms)

    def snapshot(self) -> dict:
        with self._lock:
            latencies = sorted(self._latencies_ms)
            n = len(latencies)
            p99 = latencies[int(n * 0.99)] if n else 0.0
            avg = sum(latencies) / n if n else 0.0
            histogram = {
                f"<= {bound}ms": sum(1 for latency in latencies if latency <= bound)
                for bound in LATENCY_BUCKETS_MS
            }
            return {
                "request_count": self._request_count,
                "error_count": self._error_count,
                "latency_ms_avg": round(avg, 3),
                "latency_ms_p99": round(p99, 3),
                "latency_histogram_ms": histogram,
            }
