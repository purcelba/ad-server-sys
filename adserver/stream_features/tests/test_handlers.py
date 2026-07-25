import datetime as dt

from adserver.common.events import SessionEvent
from adserver.stream_features.handlers.destination_entered import DestinationEnteredHandler
from adserver.stream_features.handlers.ride_type_selected import RideTypeSelectedHandler
from adserver.stream_features.handlers.session_start import SessionStartHandler


def _event(event_type, payload=None):
    return SessionEvent(
        event_id="e_1",
        event_type=event_type,
        user_id="u_1",
        session_id="s_1",
        ts=dt.datetime(2026, 7, 18, 12, 0, 0),
        payload=payload or {},
    )


def test_session_start_handler():
    handler = SessionStartHandler()
    assert handler.event_type() == "session_start"
    assert handler.outputs() == ["user_session_active"]
    writes = handler.update(_event("session_start"), state=None)
    assert writes == {"user_session_active": True}


def test_destination_entered_handler():
    handler = DestinationEnteredHandler()
    assert handler.event_type() == "destination_entered"
    event = _event("destination_entered", {"category": "travel", "geo": "sea"})
    writes = handler.update(event, state=None)
    assert writes == {"user_current_destination_category": "travel", "user_session_active": True}


def test_ride_type_selected_handler():
    handler = RideTypeSelectedHandler()
    event = _event("ride_type_selected", {"ride_type": "premium"})
    writes = handler.update(event, state=None)
    assert writes == {"user_current_ride_type": "premium", "user_session_active": True}
