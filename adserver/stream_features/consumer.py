"""Consumer core: auto-discovers EventHandlers, consumes from Redpanda,
dispatches by event_type, writes handler-returned features to Redis with
a shared TTL, and exposes /health + /metrics.

Owns: consumption, deserialization, dispatch, Redis writes, TTLs,
metrics. Handlers own only feature logic (stream_features/framework.py).

Unknown-event tolerance: an event_type with no registered handler is
counted and skipped — never a crash, never a stall.
"""

from __future__ import annotations

import importlib
import json
import logging
import pkgutil
import threading
import time

import redis
import uvicorn
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI

from adserver.common.events import TOPIC, SessionEvent
from adserver.stream_features import handlers as handlers_pkg
from adserver.stream_features.framework import SESSION_TTL_SECONDS, EventHandler
from adserver.stream_features.metrics import Metrics
from adserver.stream_features.state import SessionState

logger = logging.getLogger(__name__)

CONSUMER_GROUP_ID = "stream-features-consumer"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
HTTP_PORT = 8001


def discover_handlers() -> dict[str, EventHandler]:
    """Auto-discovers every EventHandler subclass under handlers/ — no
    hand-maintained list, mirroring batch_features/runner.py's
    discover_jobs(). Adding a handler module is enough; this never needs
    editing."""
    discovered: dict[str, EventHandler] = {}
    for _, module_name, _ in pkgutil.iter_modules(handlers_pkg.__path__):
        if module_name.startswith("_"):
            continue
        module = importlib.import_module(f"{handlers_pkg.__name__}.{module_name}")
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, EventHandler)
                and attr is not EventHandler
                and attr.__module__ == module.__name__
            ):
                instance = attr()
                discovered[instance.event_type()] = instance
    return discovered


def _feature_key(user_id: str, feature_name: str) -> str:
    return f"feature:user:{user_id}:{feature_name}"


def process_event(
    raw_value: bytes | str,
    handlers: dict[str, EventHandler],
    redis_client: redis.Redis,
    state: SessionState,
    metrics: Metrics,
) -> SessionEvent | None:
    """Process one raw Kafka message value end to end. Directly callable
    without a running Kafka consumer — this is what makes dispatch,
    unknown-event handling, and Redis writes unit-testable.

    Returns the parsed event on success, None if its type is unknown
    (metrics already recorded either way). Malformed JSON propagates as
    an exception — that's a producer bug, not something to silently
    swallow.
    """
    processing_start = time.time()
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")
    event = SessionEvent.from_json(raw_value)

    handler = handlers.get(event.event_type)
    if handler is None:
        metrics.record_unknown(event.event_type)
        logger.info("unknown event_type=%s, skipping", event.event_type)
        return None

    writes = handler.update(event, state)
    for feature_name, value in writes.items():
        redis_client.set(_feature_key(event.user_id, feature_name), json.dumps(value), ex=SESSION_TTL_SECONDS)

    latency_ms = (time.time() - processing_start) * 1000
    lag_seconds = time.time() - event.ts.timestamp()
    metrics.record_processed(event.event_type, latency_ms, lag_seconds)
    return event


def run_consume_loop(
    metrics: Metrics,
    stop_event: threading.Event,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    redis_host: str = DEFAULT_REDIS_HOST,
    redis_port: int = DEFAULT_REDIS_PORT,
) -> None:
    handlers = discover_handlers()
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    state = SessionState(redis_client)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    logger.info("consumer started, subscribed to %s", TOPIC)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.warning("kafka error: %s", msg.error())
                continue
            try:
                process_event(msg.value(), handlers, redis_client, state, metrics)
            except Exception:
                logger.exception("failed to process message, skipping")
    finally:
        consumer.close()
        logger.info("consumer stopped")


def create_app(metrics: Metrics) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics")
    def get_metrics():
        return metrics.snapshot()

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    metrics = Metrics()
    stop_event = threading.Event()
    thread = threading.Thread(
        target=run_consume_loop, args=(metrics, stop_event), daemon=True
    )
    thread.start()

    app = create_app(metrics)
    try:
        uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
    finally:
        stop_event.set()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
