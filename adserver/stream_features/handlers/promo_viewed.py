"""promo_viewed -> promos_viewed_10min.

Added as Phase 2's AC7 extensibility proof: this module + one
registry.yaml entry + one EVENT_TYPES entry (promo_viewed was new) is
the entire engineering change. No edits to consumer.py, framework.py,
or state.py.
"""

from __future__ import annotations

from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.framework import EventHandler
from adserver.stream_features.state import SessionState

WINDOW_SECONDS = 600  # 10 minutes


class PromoViewedHandler(EventHandler):
    def event_type(self) -> str:
        return "promo_viewed"

    def outputs(self) -> list[str]:
        return ["promos_viewed_10min", "user_session_active"]

    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        count = state.record_and_count_window(
            user_id=event.user_id,
            feature_name="promos_viewed_10min",
            member_id=event.event_id,
            ts_epoch=event.ts.timestamp(),
            window_seconds=WINDOW_SECONDS,
        )
        return {
            "promos_viewed_10min": count,
            "user_session_active": True,
        }
