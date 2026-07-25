"""Concurrent load harness for POST /features — used both by
`make loadtest-features` (against a running `service.py`) and directly by
AC4's test.

Fires `--concurrency` requests at once using a thread pool of blocking
httpx.Client calls, not asyncio.gather over one httpx.AsyncClient. Measured
directly: an async client juggling 100 in-flight coroutines does all of
each request's Python-side work (encode, parse, decode) on a single
thread — that serialized CPU work, not the server, was the dominant cost
and inflated p99 well past what `ab` (a real concurrent C client) measured
against the same server. Real OS threads let that work actually run in
parallel across cores, the same reason `ab` was closer to ground truth.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

DEFAULT_URL = "http://localhost:8003/features"
DEFAULT_QUERY = {
    "queries": [
        {"entity_type": "user", "entity_id": "u_0001", "feature_names": ["user_ctr_30d", "user_current_ride_type"]},
        {"entity_type": "ad", "entity_id": "c_0001", "feature_names": ["ad_ctr_7d"]},
    ]
}


def run_load(url: str = DEFAULT_URL, concurrency: int = 100, payload: dict = DEFAULT_QUERY) -> dict:
    """Fires `concurrency` requests at once, all in flight together (not
    one-at-a-time), and returns latency percentiles + error count."""
    client = httpx.Client()

    def _one(_) -> tuple[float, bool]:
        start = time.perf_counter()
        try:
            resp = client.post(url, json=payload)
            ok = resp.status_code == 200
        except Exception:
            ok = False
        return (time.perf_counter() - start) * 1000, ok

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(_one, range(concurrency)))
    client.close()

    latencies = sorted(latency for latency, _ in results)
    errors = sum(1 for _, ok in results if not ok)
    n = len(latencies)
    return {
        "concurrency": concurrency,
        "errors": errors,
        "latency_ms_avg": round(sum(latencies) / n, 3) if n else 0.0,
        "latency_ms_p50": round(latencies[int(n * 0.50)], 3) if n else 0.0,
        "latency_ms_p99": round(latencies[int(n * 0.99)], 3) if n else 0.0,
        "latency_ms_max": round(latencies[-1], 3) if n else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()
    run_load(url=args.url, concurrency=1)  # warm the connection pool first
    print(run_load(url=args.url, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
