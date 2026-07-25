"""Redis-backed state helper passed into EventHandler.update() calls.

Windowed state lives in Redis (not consumer in-memory) so it survives a
consumer restart — the sliding-window count a handler asks for is
correct immediately after a kill/restart, not rebuilt from scratch.
"""

from __future__ import annotations

import time

import redis


class SessionState:
    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client

    def record_and_count_window(
        self, user_id: str, feature_name: str, member_id: str, ts_epoch: float, window_seconds: int
    ) -> int:
        """Record one timestamped occurrence and return the count still
        within the trailing `window_seconds` (a Redis sorted set: ZADD +
        ZREMRANGEBYSCORE to prune + ZCARD to count)."""
        key = f"window:{user_id}:{feature_name}"
        pipe = self._redis.pipeline()
        pipe.zadd(key, {member_id: ts_epoch})
        pipe.zremrangebyscore(key, "-inf", ts_epoch - window_seconds)
        pipe.zcard(key)
        pipe.expire(key, window_seconds)
        _, _, count, _ = pipe.execute()
        return int(count)


def now_epoch() -> float:
    return time.time()
