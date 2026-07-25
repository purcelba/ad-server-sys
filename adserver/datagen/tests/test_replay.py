import datetime as dt

import numpy as np
import pytest

from adserver.datagen.replay import _ActiveSession, generate_next_event

USER_IDS = ["u_0001", "u_0002", "u_0003"]
NOW = dt.datetime(2026, 7, 18, 12, 0, 0)


def test_starts_a_new_session_when_none_active():
    rng = np.random.default_rng(1)
    active = {}
    event = generate_next_event(rng, USER_IDS, active, NOW, new_session_probability=0.0)
    assert event.event_type == "session_start"
    assert event.user_id in USER_IDS
    assert len(active) == 1


def test_continues_an_active_session():
    rng = np.random.default_rng(1)
    active = {"s1": _ActiveSession("u_0001", "s1", events_remaining=5)}
    event = generate_next_event(rng, USER_IDS, active, NOW, new_session_probability=0.0)
    assert event.event_type in {"destination_entered", "ride_type_selected", "app_screen_view"}
    assert event.user_id == "u_0001"
    assert event.session_id == "s1"


def test_session_removed_once_exhausted():
    rng = np.random.default_rng(1)
    active = {"s1": _ActiveSession("u_0001", "s1", events_remaining=1)}
    generate_next_event(rng, USER_IDS, active, NOW, new_session_probability=0.0)
    assert "s1" not in active


def test_destination_entered_payload_has_category_and_geo():
    rng = np.random.default_rng(1)
    active = {"s1": _ActiveSession("u_0001", "s1", events_remaining=5)}
    # force enough draws to eventually hit destination_entered - just check shape when it occurs
    for _ in range(20):
        active["s1"] = _ActiveSession("u_0001", "s1", events_remaining=5)
        event = generate_next_event(rng, USER_IDS, active, NOW, new_session_probability=0.0)
        if event.event_type == "destination_entered":
            assert "category" in event.payload
            assert "geo" in event.payload
            return
    pytest.fail("destination_entered never generated in 20 draws")


def test_unknown_event_rate_produces_novel_type():
    rng = np.random.default_rng(1)
    active = {}
    event = generate_next_event(rng, USER_IDS, active, NOW, unknown_event_rate=1.0)
    assert event.event_type == "__replayer_unknown_event_sentinel__"


def test_zero_unknown_event_rate_never_produces_novel_type():
    rng = np.random.default_rng(1)
    active = {}
    for _ in range(50):
        event = generate_next_event(rng, USER_IDS, active, NOW, unknown_event_rate=0.0)
        assert event.event_type != "__replayer_unknown_event_sentinel__"
