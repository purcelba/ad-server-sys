"""session_start -> user_session_active."""

from __future__ import annotations

from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.framework import EventHandler
from adserver.stream_features.state import SessionState


class SessionStartHandler(EventHandler):
    def event_type(self) -> str:
        return "session_start"

    def outputs(self) -> list[str]:
        return ["user_session_active"]

    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        return {"user_session_active": True}
