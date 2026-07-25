"""Streaming handler interface — the extensibility contract event
handlers implement. Symmetric to batch_features/framework.py's
FeatureJob: handlers own only feature logic; the consumer core
(consumer.py) owns consumption, deserialization, dispatch, Redis writes,
TTLs, and metrics.

Adding a feature from a new product event = one handler module +
registry entries (+ event schema, if the event type is new) — zero edits
to the consumer core.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from adserver.common.events import SessionEvent
from adserver.stream_features.state import SessionState

SESSION_TTL_SECONDS = 1800  # 30 minutes, per the build item — applied
# uniformly by the consumer core, not chosen per-handler.


class EventHandler(ABC):
    @abstractmethod
    def event_type(self) -> str:
        """The event_type string this handler processes."""

    @abstractmethod
    def outputs(self) -> list[str]:
        """Registry feature names this handler produces."""

    @abstractmethod
    def update(self, event: SessionEvent, state: SessionState) -> dict[str, Any]:
        """Given one event, return {feature_name: value} writes.

        Never touches Redis directly — `state` exposes only the specific
        stateful operations a handler needs (e.g. sliding-window
        counting); the consumer core applies the returned writes with the
        shared TTL policy.
        """
