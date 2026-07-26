"""Load harness for POST /serve, paced at a target requests-per-second
rate (not a single all-at-once burst) — AC1 literally asks for "load test
at 50 RPS." Uses a thread pool of blocking httpx.Client calls, the same
approach feature_service/loadtest.py already proved more reliable than
asyncio.gather for this kind of latency measurement (the async client's
own single-threaded coroutine bookkeeping distorts p99 at concurrency).
"""

from __future__ import annotations

import argparse
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

DEFAULT_URL = "http://localhost:8005/serve"
N_SYNTHETIC_USERS = 50


def _random_payload() -> dict:
    return {"user_id": f"u_{random.randint(1, N_SYNTHETIC_USERS):04d}", "session_id": str(uuid.uuid4())}


def run_load(url: str = DEFAULT_URL, rps: float = 50.0, duration_sec: float = 10.0, max_workers: int = 25) -> dict:
    """Submits one request every `1/rps` seconds for `duration_sec`
    seconds, each running concurrently in a thread pool — a genuine
    sustained-rate load, not a burst."""
    # httpx.Client()'s default Limits (max_connections=100,
    # max_keepalive_connections=20) throttled this client well below what
    # `ab` (a real concurrent C client) measured against the identical
    # server - raised so the client's own connection pool is never what's
    # being measured.
    client = httpx.Client(limits=httpx.Limits(max_connections=max_workers, max_keepalive_connections=max_workers))
    results: list[tuple[float, bool]] = []
    lock = threading.Lock()

    def _one() -> None:
        start = time.perf_counter()
        try:
            resp = client.post(url, json=_random_payload(), timeout=5.0)
            ok = resp.status_code == 200
        except httpx.HTTPError:
            ok = False
        elapsed_ms = (time.perf_counter() - start) * 1000
        with lock:
            results.append((elapsed_ms, ok))

    # Scheduled against an absolute start time (not a repeated
    # time.sleep(interval)) - under thread contention, a submitting
    # thread that oversleeps even slightly on every iteration accumulates
    # drift, and a naive fixed-interval sleep loop has no way to catch up
    # other than bursting later submissions, which is exactly the kind of
    # self-inflicted overload this loader is trying to avoid measuring.
    interval = 1.0 / rps
    n_requests = int(rps * duration_sec)
    schedule_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for i in range(n_requests):
            target_time = schedule_start + i * interval
            sleep_for = target_time - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            pool.submit(_one)
    client.close()

    latencies = sorted(latency for latency, _ in results)
    errors = sum(1 for _, ok in results if not ok)
    n = len(latencies)
    return {
        "n_requests": n_requests,
        "rps": rps,
        "duration_sec": duration_sec,
        "errors": errors,
        "latency_ms_avg": round(sum(latencies) / n, 3) if n else 0.0,
        "latency_ms_p50": round(latencies[int(n * 0.50)], 3) if n else 0.0,
        "latency_ms_p99": round(latencies[int(n * 0.99)], 3) if n else 0.0,
        "latency_ms_max": round(latencies[-1], 3) if n else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--rps", type=float, default=50.0)
    parser.add_argument("--duration-sec", type=float, default=10.0)
    args = parser.parse_args()
    print(run_load(url=args.url, rps=args.rps, duration_sec=args.duration_sec))


if __name__ == "__main__":
    main()
