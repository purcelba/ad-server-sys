# ops

## What it does
The measurement loop that closes over Phase 5's decision log
(`data/decision_log.jsonl`), the system of record every `/serve` request
appends to. Three batch tools, all reading that one file — no dedicated
infra, just plain Python reading JSON lines and Parquet:

- **`reconcile.py`** (`make reconcile`) — compares what pacing's Redis
  counters *think* each campaign delivered against what the decision log
  says actually happened, per campaign. A non-zero discrepancy is
  expected, not a bug in this report: `pacing.decrement_capacity()` is a
  deliberate best-effort `GET`-then-`SET`, not an atomic decrement (see
  `adserver/adserver/pacing.py`'s docstring), and Phase 5's AC4
  demonstrated the exact concurrent-request race that causes it. This is
  the "accept and reconcile" leg of that documented tradeoff — a
  production system would instead fix the counter itself (an atomic
  `DECRBY`/Lua script, or a reservation with a rollback path); this
  project makes the drift visible instead.

- **`outcomes.py`** — not a runnable tool, the shared building block the
  other two depend on. **No click-tracking exists anywhere in this
  project** — `/serve` only logs a served impression, never a subsequent
  click. `simulate_click()` retroactively labels a logged decision using
  `datagen/lifts.py::click_probability(segment, category, hour_of_day)` —
  the exact generative model behind Phase 0's original synthetic
  `events.parquet` history, not a new signal. One implementation, used by
  both `ranking/retrain.py` and `readout.py`, so they can never silently
  diverge on what counts as a "click." Every result derived from it is
  documented as simulated feedback, not real user behavior.

- **`readout.py`** (`make readout`) — groups decisions by
  `experiment_arm`, simulates a click per decision, and reports per-arm
  CTR with a Wilson-score 95% confidence interval. Only decisions whose
  winner is a real, scored campaign count as an impression (matching
  `ranking/retrain.py`'s definition — a no-fill decision showed nothing,
  and a house-ad fallback was never eligible to be won by either model
  version, so including it would dilute both arms without saying
  anything about the models being compared).

(`ranking/retrain.py` — the third measurement-loop consumer, training a
new model version directly from the decision log — lives under
`ranking/` alongside `train.py`, not here; see `ranking/README.md`.)

**Why observational arm comparison is trustworthy here.** Every
request's arm is assigned by `experiment.assign_arm()` — a deterministic
hash of `(salt, user_id)` (Phase 5) — independent of anything about the
request, the candidate set, or the user's behavior. That randomization is
what lets a plain per-arm CTR difference be attributed to the model
version rather than to who happened to land in which arm; no matched
cohort or regression adjustment is needed. What would break this trust:
assignment drift (the salt or the arm-split ratio changing mid-experiment,
so the two arms stop being comparable populations), or logging loss
correlated with arm (e.g. one version's slower scoring path timing out
and dropping decisions more often than the other). Neither applies to
this project's fixed-salt, single-pipeline setup — but they're exactly
what a real experimentation platform's automated pre-checks (sample ratio
mismatch, pre-period A/A tests) exist to catch before anyone trusts a
readout.

**Where this stops working: incrementality for brand campaigns.** A CTR
lift says which ranking produces more clicks among people who were
already going to see *an* ad — it says nothing about whether the ad
caused any of those clicks, which is the actual question for a brand
campaign optimizing for incremental awareness or conversion rather than
direct response. Answering that needs a holdout group who would have
been eligible but never sees the campaign at all, compared against
exposed users — a different, harder randomization than "which model
scored you." Explicitly out of this project's build scope.

## How to run and test it alone
```bash
uv run pytest adserver/ops -v                       # unit tests, incl. the AC3 synthetic-fixture
                                                       # planted-difference detection test - no infra needed

# against a real decision log (needs make up + a running feature_service/
# bidder_stub/adserver, then real /serve traffic, e.g. via
# adserver/adserver/loadtest.py, to populate data/decision_log.jsonl):
make reconcile
make retrain
make readout
```

## Production analog
`reconcile.py` is the kind of nightly/hourly batch job a data-quality or
finance-adjacent team runs to catch budget/delivery drift — the same
shape as a real ad system's delivery reconciliation against a billing
ledger. `readout.py` is a miniature, single-file stand-in for an
experimentation platform's readout job (e.g. what a Statsig/internal
experimentation service computes automatically per metric per arm) —
real systems add sequential-testing corrections, multiple-comparison
correction across many metrics, and automated guardrail/SRM checks this
project's single planned metric doesn't need. `outcomes.py`'s simulated
labels stand in for a real click-tracking pipeline (an event like
`events.parquet`'s `click` rows, joined back to the impression that
served it) that this project never built.

## Ownership note
`reconcile.py` and `readout.py` are core ads-team territory under an
end-to-end ownership model — delivery accuracy and experiment readouts
are decisions the team shipping ranking changes needs to see directly,
not receive secondhand from a platform team. `ranking/retrain.py` (see
`ranking/README.md`) is the same ads-ML ownership as the rest of
`ranking/`. A real click-tracking pipeline, if this project had one,
would more plausibly be negotiable — either owned centrally (a shared
event pipeline every product surface, not just ads, emits into) or by
the ads team if ad clicks need ads-specific enrichment before they're
usable — the kind of tradeoff `outcomes.py`'s existence here is
deliberately standing in for.
