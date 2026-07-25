"""app_screen_view -> user_screens_viewed_10min.

The one handler that uses `state`: a 10-minute sliding-window count,
kept in Redis (not consumer in-memory) so it survives a restart.
"""

from __future__ import annotations

from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.framework import EventHandler
from adserver.stream_features.state import SessionState

WINDOW_SECONDS = 600  # 10 minutes


class AppScreenViewHandler(EventHandler):
    def event_type(self) -> str:
        return "app_screen_view"

    def outputs(self) -> list[str]:
        return ["user_screens_viewed_10min", "user_session_active"]

    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        count = state.record_and_count_window(
            user_id=event.user_id,
            feature_name="user_screens_viewed_10min",
            member_id=event.event_id,
            ts_epoch=event.ts.timestamp(),
            window_seconds=WINDOW_SECONDS,
        )
        return {
            "user_screens_viewed_10min": count,
            "user_session_active": True,
        }
