# bidder_stub

## What it does
A fake external programmatic bidder — not real auction logic, a
controllable dependency for exercising the ad server's own failure-mode
handling. `POST /bid` returns a bid (a random value in a fixed range)
after sleeping for a sampled latency (`gauss(mean_ms, std_ms)`, floored at
0) and, with configurable probability, raising a `503` instead of
returning a bid at all. Both the latency distribution and failure rate
have env-var defaults (`BIDDER_LATENCY_MEAN_MS`, `BIDDER_LATENCY_STD_MS`,
`BIDDER_FAILURE_RATE`) and per-request query-param overrides, so a test
can force above-timeout latency or a guaranteed failure for one call
without restarting the process. `/health` and `/metrics` (request count,
error count, latency histogram) follow the same shape every other service
in this project uses.

The ad server (`adserver/adserver/`) calls `POST /bid` with a hard 30ms
`httpx` timeout; a timeout or `503` means internal demand only for that
request, logged as such — never a failure the caller has to handle
specially beyond "no external bid this time."

## How to run and test it alone
```bash
uv run pytest adserver/bidder_stub -v
uv run python -m adserver.bidder_stub.service   # serves :8004

curl -X POST localhost:8004/bid -H 'Content-Type: application/json' \
  -d '{"request_id": "r1", "user_id": "u_0001"}'

# force a guaranteed 503, for testing the ad server's timeout/failure path:
curl -X POST 'localhost:8004/bid?failure_rate=1.0' -H 'Content-Type: application/json' \
  -d '{"request_id": "r1", "user_id": "u_0001"}'
```

## Production analog
This is the local stand-in for an external demand-side platform (DSP) or
programmatic exchange integration — a real ad server calls out to one or
more of these over the network, with exactly the same shape of problem
this stub models: unpredictable latency, occasional unavailability, and a
hard timeout budget because the caller's own SLO doesn't bend to
someone else's infrastructure.

## Ownership note
Under an end-to-end ads team model, this is negotiable — a thin external-
integration shim like this could plausibly sit with the ads team (it's
part of the serving critical path) or a central platform/integrations
team (it's a generic "call an external partner with a timeout" pattern,
not ads-specific logic). Low stakes either way since it's intentionally
dumb — the real complexity is in how the ad server *uses* it, not in the
stub itself.
