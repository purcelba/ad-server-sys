"""Replayer — the load and test harness for Phase 2's real-time path.

Streams seeded synthetic session events at a configurable rate, publishing
via the same event schema (common/events.py) and topic the mini page
uses — the consumer cannot tell them apart. Exists for volume,
repeatability, and headless use; every automated acceptance criterion
needing sustained traffic (kill test, load tests) runs on this, not the
mini page.

See stream_features/README.md's "Replayer vs. mini page" section.
"""

from __future__ import annotations

import datetime as dt
import time
import uuid
from pathlib import Path

import numpy as np
import polars as pl
import typer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from adserver.common.events import TOPIC, SessionEvent
from adserver.datagen.lifts import CATEGORIES
from adserver.datagen.rides import RIDE_TYPES

app = typer.Typer(add_completion=False)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
SCREENS = ["home", "search", "trip_history", "account", "promotions"]
MIDDLE_EVENT_TYPES = ["destination_entered", "ride_type_selected", "app_screen_view"]


def ensure_topic(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS) -> None:
    """Idempotent topic creation, mirroring batch_features/materialize.py's
    create_table_if_not_exists — avoids relying on (and the transient
    error from) Redpanda's auto-create-on-first-produce."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if TOPIC in existing:
        return
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    for topic, future in futures.items():
        future.result()  # raises on failure, no-op on success


def _payload_for(event_type: str, rng: np.random.Generator) -> dict:
    if event_type == "destination_entered":
        return {"category": CATEGORIES[int(rng.integers(0, len(CATEGORIES)))], "geo": "sea"}
    if event_type == "ride_type_selected":
        return {"ride_type": RIDE_TYPES[int(rng.integers(0, len(RIDE_TYPES)))]}
    if event_type == "app_screen_view":
        return {"screen": SCREENS[int(rng.integers(0, len(SCREENS)))]}
    return {}


class _ActiveSession:
    def __init__(self, user_id: str, session_id: str, events_remaining: int):
        self.user_id = user_id
        self.session_id = session_id
        self.events_remaining = events_remaining


def generate_next_event(
    rng: np.random.Generator,
    user_ids: list[str],
    active_sessions: dict[str, _ActiveSession],
    now: dt.datetime,
    new_session_probability: float = 0.15,
    unknown_event_rate: float = 0.0,
) -> SessionEvent:
    """One event: either starts a new session, continues an active one, or
    (if `unknown_event_rate` fires) emits a novel event type — used to
    exercise unknown-event tolerance (AC6).

    The injected type is a permanent sentinel, deliberately never added
    to EVENT_TYPES or given a handler — unlike promo_viewed (Phase 2
    AC7's extensibility proof), which started as an unknown-event stand-in
    but stopped being "unknown" once it got a real handler. Reusing a
    type that might later become real would silently break this test on
    a future re-run.
    """
    if unknown_event_rate > 0 and rng.uniform(0.0, 1.0) < unknown_event_rate:
        user_id = user_ids[int(rng.integers(0, len(user_ids)))]
        return SessionEvent(
            event_id=str(uuid.uuid4()),
            event_type="__replayer_unknown_event_sentinel__",
            user_id=user_id,
            session_id=str(uuid.uuid4()),
            ts=now,
            payload={"note": "deliberately unrecognized, for testing unknown-event tolerance"},
        )

    start_new = (not active_sessions) or (rng.uniform(0.0, 1.0) < new_session_probability)
    if start_new:
        user_id = user_ids[int(rng.integers(0, len(user_ids)))]
        session_id = str(uuid.uuid4())
        events_remaining = int(rng.integers(2, 6))
        active_sessions[session_id] = _ActiveSession(user_id, session_id, events_remaining)
        return SessionEvent(
            event_id=str(uuid.uuid4()),
            event_type="session_start",
            user_id=user_id,
            session_id=session_id,
            ts=now,
            payload={},
        )

    session_id = list(active_sessions.keys())[int(rng.integers(0, len(active_sessions)))]
    session = active_sessions[session_id]
    event_type = MIDDLE_EVENT_TYPES[int(rng.integers(0, len(MIDDLE_EVENT_TYPES)))]
    session.events_remaining -= 1
    if session.events_remaining <= 0:
        del active_sessions[session_id]

    return SessionEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        user_id=session.user_id,
        session_id=session.session_id,
        ts=now,
        payload=_payload_for(event_type, rng),
    )


def run_replay(
    events_per_sec: float,
    duration_sec: float,
    seed: int = 42,
    data_dir: Path = Path("data"),
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    compression_factor: float = 1.0,
    unknown_event_rate: float = 0.0,
) -> int:
    """Publish events for `duration_sec` wall-clock seconds at
    `events_per_sec`. `compression_factor` > 1 advances each event's `ts`
    faster than wall-clock (for generating volume/history quickly); has
    no effect on actual publish timing, so live-latency ACs should use
    compression_factor=1.0 (the default).

    Returns the number of events published.
    """
    ensure_topic(bootstrap_servers)
    users = pl.read_parquet(data_dir / "users.parquet")
    user_ids = users["user_id"].to_list()

    rng = np.random.default_rng(seed)
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    active_sessions: dict[str, _ActiveSession] = {}

    interval = 1.0 / events_per_sec
    start_wall = time.time()
    published = 0

    while (time.time() - start_wall) < duration_sec:
        elapsed = time.time() - start_wall
        simulated_ts = dt.datetime.now() + dt.timedelta(seconds=elapsed * (compression_factor - 1.0))
        event = generate_next_event(
            rng, user_ids, active_sessions, simulated_ts, unknown_event_rate=unknown_event_rate
        )
        producer.produce(TOPIC, value=event.to_json().encode("utf-8"))
        producer.poll(0)
        published += 1
        time.sleep(interval)

    producer.flush(10)
    return published


@app.command()
def main(
    events_per_sec: float = typer.Option(50.0, help="Publish rate."),
    duration_sec: float = typer.Option(60.0, help="How long to replay, in wall-clock seconds."),
    seed: int = typer.Option(42, help="RNG seed for user/event selection."),
    data_dir: Path = typer.Option(Path("data"), help="Directory with users.parquet."),
    bootstrap_servers: str = typer.Option(DEFAULT_BOOTSTRAP_SERVERS),
    compression_factor: float = typer.Option(1.0, help="Simulated-time speedup; 1.0 = real-time."),
    unknown_event_rate: float = typer.Option(0.0, help="Probability of injecting a novel event type."),
) -> None:
    count = run_replay(
        events_per_sec,
        duration_sec,
        seed=seed,
        data_dir=data_dir,
        bootstrap_servers=bootstrap_servers,
        compression_factor=compression_factor,
        unknown_event_rate=unknown_event_rate,
    )
    typer.echo(f"Published {count} events to {TOPIC}")


if __name__ == "__main__":
    app()
