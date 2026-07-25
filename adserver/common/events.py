"""Session event schema — the shared contract between producers (the
replayer, the mini page) and the consumer, so they can't drift. This is
the "event schema in common/" the extension runbook's step 2 refers to.

Both event sources publish this exact schema to the same Kafka topic;
the consumer cannot tell them apart, by design.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime

TOPIC = "session_events"

EVENT_TYPES = {"session_start", "destination_entered", "ride_type_selected", "app_screen_view"}


class EventValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SessionEvent:
    event_id: str
    event_type: str
    user_id: str
    session_id: str
    ts: datetime
    payload: dict

    def to_json(self) -> str:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        return json.dumps(d)

    @staticmethod
    def from_json(raw: str) -> "SessionEvent":
        d = json.loads(raw)
        return SessionEvent(
            event_id=d["event_id"],
            event_type=d["event_type"],
            user_id=d["user_id"],
            session_id=d["session_id"],
            ts=datetime.fromisoformat(d["ts"]),
            payload=d.get("payload", {}),
        )


def validate_for_publish(event: SessionEvent) -> None:
    """Producers call this before publishing — the consumer does NOT
    enforce EVENT_TYPES, since it must tolerate types it doesn't
    recognize (the unknown-event tolerance build item)."""
    if event.event_type not in EVENT_TYPES:
        raise EventValidationError(
            f"event_type {event.event_type!r} is not a known type — must be one of {EVENT_TYPES}"
        )
    if not event.event_id or not event.user_id or not event.session_id:
        raise EventValidationError("event_id, user_id, and session_id are all required")
