"""destination_entered -> user_current_destination_category.

Every handler also refreshes user_session_active=True — any event means
the session is still active, per the build item.
"""

from __future__ import annotations

from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.framework import EventHandler
from adserver.stream_features.state import SessionState


class DestinationEnteredHandler(EventHandler):
    def event_type(self) -> str:
        return "destination_entered"

    def outputs(self) -> list[str]:
        return ["user_current_destination_category", "user_session_active"]

    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        return {
            "user_current_destination_category": event.payload["category"],
            "user_session_active": True,
        }
