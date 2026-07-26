"""Thread-safe metrics: total request/error counts + latency histogram
(same shape as every other service's /metrics), plus per-stage latency
(retrieval/features/scoring/bidder/total) — phases.md's Phase 5 build item
explicitly asks for per-stage visibility, not just an aggregate.
"""

from __future__ import annotations

import threading
from collections import deque

LATENCY_BUCKETS_MS = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1000]
STAGES = ["retrieval", "features", "scoring", "bidder", "total"]


class Metrics:
    def __init__(self, latency_window: int = 1000):
        self._lock = threading.Lock()
        self._request_count = 0
        self._error_count = 0
        self._fallback_counts: dict[str, int] = {}
        self._stage_latencies_ms: dict[str, deque[float]] = {stage: deque(maxlen=latency_window) for stage in STAGES}

    def record_request(self, stage_latencies_ms: dict[str, float], error: bool = False, fallback_rung: str | None = None) -> None:
        with self._lock:
            self._request_count += 1
            if error:
                self._error_count += 1
            if fallback_rung:
                self._fallback_counts[fallback_rung] = self._fallback_counts.get(fallback_rung, 0) + 1
            for stage, latency_ms in stage_latencies_ms.items():
                if stage in self._stage_latencies_ms:
                    self._stage_latencies_ms[stage].append(latency_ms)

    def snapshot(self) -> dict:
        with self._lock:
            stages = {}
            for stage, latencies in self._stage_latencies_ms.items():
                sorted_latencies = sorted(latencies)
                n = len(sorted_latencies)
                p99 = sorted_latencies[int(n * 0.99)] if n else 0.0
                avg = sum(sorted_latencies) / n if n else 0.0
                stages[stage] = {"latency_ms_avg": round(avg, 3), "latency_ms_p99": round(p99, 3)}

            return {
                "request_count": self._request_count,
                "error_count": self._error_count,
                "fallback_rung_counts": dict(self._fallback_counts),
                "stage_latency_ms": stages,
            }
