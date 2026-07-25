"""ride_type_selected -> user_current_ride_type.

Not named in the build item's explicit feature list, but ride_type_selected
is a documented event type in the schema, so it gets a real handler
rather than being miscategorized as "unknown"."""

from __future__ import annotations

from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.framework import EventHandler
from adserver.stream_features.state import SessionState


class RideTypeSelectedHandler(EventHandler):
    def event_type(self) -> str:
        return "ride_type_selected"

    def outputs(self) -> list[str]:
        return ["user_current_ride_type", "user_session_active"]

    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        return {
            "user_current_ride_type": event.payload["ride_type"],
            "user_session_active": True,
        }
