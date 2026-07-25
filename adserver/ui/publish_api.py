"""Mini page publish endpoint — the precision instrument for Phase 2's
real-time path. Serves the static form (ui/mini.html) and POST /events,
which builds a SessionEvent via the same common/events.py constructors
the replayer uses and publishes to the same topic. The consumer cannot
tell the two sources apart — see stream_features/README.md's "Replayer
vs. mini page" section.
"""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path

import polars as pl
import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from adserver.common.events import TOPIC, EventValidationError, SessionEvent, validate_for_publish
from adserver.datagen.replay import DEFAULT_BOOTSTRAP_SERVERS, ensure_topic

HTTP_PORT = 8002
MINI_HTML_PATH = Path(__file__).parent / "mini.html"


class FireEventRequest(BaseModel):
    user_id: str
    event_type: str
    payload: dict = {}


def create_app(data_dir: Path = Path("data"), bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS) -> FastAPI:
    app = FastAPI()
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    ensure_topic(bootstrap_servers)

    users = pl.read_parquet(data_dir / "users.parquet")
    user_ids = sorted(users["user_id"].to_list())
    options_html = "\n".join(f'<option value="{uid}">{uid}</option>' for uid in user_ids)
    mini_html = MINI_HTML_PATH.read_text().replace("{{USER_OPTIONS}}", options_html)

    @app.get("/", response_class=HTMLResponse)
    def index():
        return mini_html

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/events")
    def publish_event(req: FireEventRequest):
        event = SessionEvent(
            event_id=str(uuid.uuid4()),
            event_type=req.event_type,
            user_id=req.user_id,
            session_id=str(uuid.uuid4()),
            ts=dt.datetime.now(),
            payload=req.payload,
        )
        try:
            validate_for_publish(event)
        except EventValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        try:
            producer.produce(TOPIC, value=event.to_json().encode("utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"failed to publish: {exc}") from exc
        producer.flush(5)
        return {"published": True, "event_id": event.event_id, "event_type": event.event_type}

    return app


def main() -> None:
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()
