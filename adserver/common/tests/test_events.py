import datetime as dt

import pytest

from adserver.common.events import EventValidationError, SessionEvent, validate_for_publish


def _event(**overrides):
    defaults = dict(
        event_id="e_1",
        event_type="destination_entered",
        user_id="u_0001",
        session_id="s_1",
        ts=dt.datetime(2026, 7, 18, 12, 0, 0),
        payload={"category": "travel", "geo": "sea"},
    )
    defaults.update(overrides)
    return SessionEvent(**defaults)


def test_json_roundtrip_preserves_all_fields():
    event = _event()
    restored = SessionEvent.from_json(event.to_json())
    assert restored == event


def test_json_roundtrip_preserves_payload_dict():
    event = _event(payload={"screen": "home", "duration_ms": 1200})
    restored = SessionEvent.from_json(event.to_json())
    assert restored.payload == {"screen": "home", "duration_ms": 1200}


def test_validate_accepts_known_event_type():
    validate_for_publish(_event(event_type="session_start"))  # must not raise


def test_validate_rejects_unknown_event_type():
    with pytest.raises(EventValidationError, match="not_a_real_type"):
        validate_for_publish(_event(event_type="not_a_real_type"))


def test_validate_rejects_missing_ids():
    with pytest.raises(EventValidationError):
        validate_for_publish(_event(user_id=""))
