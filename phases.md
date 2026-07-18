# Toy Ad Server — Phased Build Spec

A learning project: a miniature real-time ad serving system combining batch and real-time features, built with Claude Code. Goal is concept fidelity to production ad serving (feature stores, streaming, two-stage ranking, pacing, yield across demand types, degradation, decision logging, experimentation) at local scale — not scale fidelity.

**How to use this spec with Claude Code:** work one phase at a time. Give Claude Code the phase section plus the "Global conventions" section. Each phase ends with acceptance criteria — do not move on until they pass. Phases are ordered so every phase produces something runnable.

**Tracking:** every build item and acceptance criterion below has a checkbox. Check them off as they land; tag the repo (`phase-0`, `phase-1`, ...) once a phase's boxes are all checked, per the version-control convention below.

---

## Global conventions

- **Language/runtime:** Python 3.12, managed with `uv`. One monorepo.
- **Services communicate only via HTTP, the event stream, or the online store** — never by importing each other's code. Shared code lives in a small `common/` package (schemas, registry loader, metrics helpers) only.
- **Infra via Docker Compose:** Redpanda (Kafka API), DynamoDB-local, Redis. Everything else is a Python process.
- **Repo layout:**
  ```
  adserver/
    common/            # shared schemas, feature registry loader, metrics
    datagen/           # synthetic data generator
    batch_features/    # batch pipeline (Polars + DuckDB + Parquet)
    stream_features/   # event consumer computing real-time features
    feature_service/   # online feature retrieval API
    ranking/           # training pipeline, model registry, scorer
    adserver/          # the serving endpoint: retrieval, auction, pacing, fallbacks
    bidder_stub/       # fake external programmatic bidder
    ops/               # reconciliation job, ops dashboard, chaos scripts
    ui/                # debug frontend
    tests/             # cross-component integration + failure-mode tests
  ```
- **Version control:** GitHub repo from Phase 0, before any other code. Commit at least once per acceptance criterion passed; tag the completion of each phase (`phase-0`, `phase-1`, ...) so any phase's state is recoverable and the build history tells the learning story. Conventional, descriptive commit messages — Claude Code writes these well when asked. A lightweight CI (GitHub Actions running `make test` on push) is worth the 20 minutes: it enforces the "components testable in isolation" promise and is itself on-theme for owning infra. Keep the repo personal and private, built entirely on personal accounts and equipment before the start date — it's a learning exercise using only public information, but keep Lyft's name out of the repo name and code (call it `toy-adserver` or similar), and treat it as portfolio/prep material, not something to import into work later.
- **Every service** exposes `/health` and `/metrics` (JSON: request count, error count, latency histogram buckets). No Prometheus stack — keep it inspectable with curl.
- **Every component README** must contain: (1) what it does, (2) how to run and test it alone, (3) *production analog* at Lyft-scale (e.g. "this is Kinesis + Flink", "this is the Feature Service backed by DynamoDB/Redis"), and (4) *ownership note*: under an end-to-end ads team model, is this component plausibly owned by the ads team, the ML/data platform, or negotiable — and what the tradeoff is.
- **Testing hierarchy:** unit tests per component; integration tests in `tests/`; failure-mode tests (dependency down → correct fallback fires AND is logged) are first-class and required where specified.
- **Explicitly out of scope** (do not build, note in README where relevant): exactly-once semantics, consensus/multi-region, Kubernetes, real auction theory (second-price mechanics beyond the basics), attribution modeling, real OpenRTB protocol compliance.

---

## Phase 0 — Skeleton and synthetic world

**Goal:** a repo that runs end to end as stubs, plus a synthetic data generator with planted signal.

**Build:**
- [x] Repo scaffold per layout above; Docker Compose file bringing up Redpanda, DynamoDB-local, Redis; `make up`, `make test`, `make demo` targets.
- [x] `datagen/`: generates (a) a catalog of ~50 users with segments (commuter, traveler, nightlife, etc.), (b) ~40 ad campaigns across two demand types — `auction` (bid, budget) and `guaranteed` (impression goal, flight dates) — with categories (food, retail, entertainment, travel, transit), (c) 30 days of historical impression/click logs where clicks follow planted preferences (e.g. traveler segment clicks travel ads at 3x base rate; nightlife segment clicks food/entertainment at night), and (d) 30 days of ride history with a planted segment-dependent frequency (`rides.parquet` — added during Phase 1 planning as a flagged amendment, to give Phase 1's `user_rides_per_week` feature a real data source; see `PROGRESS.md`). Seeded and deterministic.

**Key decisions locked:** Python + uv; Polars for transforms; the planted-signal correlations documented in `datagen/README.md` so model evals can be sanity-checked against ground truth.

**Acceptance criteria:**
- [x] 1. `make up` brings up all infra containers healthy.
- [x] 2. `datagen` produces users.parquet, campaigns.parquet, events.parquet, rides.parquet deterministically (same seed → identical files).
- [x] 3. A written table in the README of planted effects (segment × category lift factors).
- [x] 4. EDA visualizations generated from datagen output: CTR by (user segment × campaign category), and CTR by (user segment × time of day) — both saved as PNGs and visually consistent with the planted lift table.

---

## Phase 1 — Feature registry + batch feature pipeline

**Goal:** batch features computed from history, materialized to the online store, with the registry as a governed contract.

**Build:**
- [x] `common/registry.yaml`: every feature declared with name, entity (user|ad), dtype, description, aggregation + window, freshness SLA (e.g. "24h"), default value, owner. Loader validates the file; CI-style test fails on missing fields.
- [x] `batch_features/`: Polars jobs computing ~8 features (examples: user_ctr_by_category_30d, user_rides_per_week, ad_ctr_7d, ad_impressions_7d, campaign_spend_yesterday). Offline store = date-partitioned Parquet queried via DuckDB, supporting point-in-time reads ("features as of date D").
- [x] **Feature-job interface (extensibility contract):** every batch job implements a common shape — `outputs()` (registry names it produces), `compute(date_range) -> frame` — with validation and materialization provided by the shared framework, not the job. The pipeline runner discovers registered jobs; adding a feature family means adding one job module + registry entries, with zero edits to existing jobs or the runner. This is the seam future extensions plug into (e.g. LLM-derived creative features, embeddings), and the streaming consumer's outputs honor the same registry contract, so compute location (batch vs streaming) is swappable per feature.
- [x] Materialization job writing latest values to DynamoDB-local (store of record) with computed_at timestamps.
- [x] Data-quality gate: row-count and null-rate checks; failures block materialization with a clear error. Policy documented.
- [x] **Audiences (sellable segments):** an `audiences.yaml` defining named audiences (e.g. `frequent_airport_travelers`, `weekday_commuters`) as rules over user features, each carrying a `definition_version`. A batch job (implementing the standard feature-job interface) evaluates definitions and materializes memberships as a user feature (`audience_memberships: list[str]`). Changing a definition bumps its version and is logged — memberships silently drifting under an unchanged name is the advertiser-trust failure this prevents. A small reach report (`make reach`) prints member counts and pairwise overlap per audience from the offline store — the "how many riders would this campaign reach?" question a sales team asks before selling.

**Acceptance criteria:**
- [x] 1. `make features` computes and materializes all registry features; re-running is idempotent.
- [ ] 2. Point-in-time test: querying features "as of" day 15 returns values computed only from days 1–15.
- [ ] 3. Poisoning test: corrupt a day's events (inject 50% nulls) → pipeline fails at the quality gate and does NOT materialize.
- [ ] 4. Every materialized item carries computed_at; nothing outside the registry gets materialized.
- [ ] 5. Extensibility proof: add a trivial new feature job (e.g. user_account_age_days) touching only its own module + registry entries — runner picks it up, quality gate applies, feature is served by Phase 3 with no other code changes. Keep this commit small as evidence the seam works.

---

## Phase 2 — Real-time feature path

**Goal:** session events flow through the broker into fresh online features, with lag observable and failure behavior tested.

**Build:**
- [ ] Event schema: session events (`session_start`, `destination_entered` {category, geo}, `ride_type_selected`, `app_screen_view`).
- [ ] `stream_features/consumer.py`: consumes from Redpanda, maintains windowed session aggregates (current destination category, screens viewed last 10 min, session active flag), writes to Redis with TTL (30 min). Registry entries for these features declare freshness SLA in seconds.
- [ ] **Streaming handler interface (extensibility contract, symmetric to the Phase 1 batch-job interface):** each event type is handled by a registered handler declaring `event_type()`, `outputs()` (registry feature names), and `update(event, state) -> feature writes`. The consumer framework owns consumption, deserialization, dispatch-by-type, Redis writes, TTLs, and metrics — handlers own only the feature logic. Adding a feature from a new product event = one handler module + registry entries + (if new) the event schema; zero edits to the consumer core or any downstream service. The live model ignores new features until a future version's config includes them.
- [ ] **Unknown-event tolerance (required):** events with unrecognized types are skipped, counted, and surfaced in `/metrics` as `unknown_events_by_type` — never a crash, never a stall. This is the forward-compatibility posture for product teams shipping events before this pipeline handles them; the unknown-event counter is also the discovery mechanism for new signals worth building features from.
- [ ] Consumer `/metrics`: events consumed, processing latency, and lag (latest event timestamp minus last processed event timestamp).
- [ ] **Two event sources, by design — build both:**
  - *Replayer* (`datagen/replay.py`): streams seeded synthetic session events at a configurable rate with optional timestamp compression (simulate days in minutes). This is the **load and test harness** — it exists for volume, repeatability, and headless CI use. All automated acceptance criteria that need sustained traffic (kill test, load tests, pacing convergence, experiment power) run on the replayer.
  - *Mini event page* (`ui/mini.html` + a tiny publish endpoint): a single HTML form — user dropdown, destination category, ride type, "fire event" button — that publishes one real event to Redpanda, exactly as an app client would. This is the **precision instrument** — for firing a single, deliberate event by hand and watching it propagate (Redis, feature service, and later `/serve`). It is the primary tool for manual verification and demos from Phase 2 onward; the Phase 7 Rider tab extends it, not replaces the replayer.
  - Both sources publish the identical event schema to the identical topic; the consumer cannot tell them apart. A README section titled "Replayer vs. mini page" states this division of labor explicitly.

**Key decisions locked:** at-least-once delivery accepted (README explains double-count implications and why serving logs, not the stream, are the measurement source of truth); plain Python consumer, no Flink — README maps concepts (event time vs processing time, windows, state, watermarks) to where each appears in the code.

**Acceptance criteria:**
- [ ] 1. Replaying events at 50 events/sec → destination_category feature visible in Redis within 2 seconds of the event.
- [ ] 2. TTL test: 30 min after session events stop (clock-mockable), session features are absent, not stale.
- [ ] 3. **Kill test (required):** stop the consumer 2 minutes mid-replay, restart → consumer catches up; lag metric visibly spikes and recovers; a test asserts recovery and no crash.
- [ ] 4. Lag and throughput visible via `/metrics` while replaying.
- [ ] 5. Manually fired event from the mini page is visible in Redis (correct feature, correct TTL) within 2 seconds — verified by hand and by one automated test hitting the publish endpoint.
- [ ] 6. Unknown-event test: replay a stream containing a novel event type → consumer neither crashes nor stalls, known features keep updating, and the unknown type appears in the metrics counter.
- [ ] 7. Streaming extensibility proof: add a handler for a new event type (e.g. `promo_viewed` → `promos_viewed_10min`) touching only its own handler module + registry entries (+ event schema); the feature flows through to the Phase 3 feature service with no other code changes. Keep the commit small as evidence, mirroring the Phase 1 proof.

---

## Phase 3 — Feature retrieval service

**Goal:** one governed API assembling batch + real-time features with freshness semantics.

**Build:**
- [ ] `feature_service/` (FastAPI): `POST /features` with entity ids + feature names → values, plus per-feature metadata: computed_at, freshness_status (`fresh` | `stale` | `missing`, judged against the registry SLA), and whether a default was substituted. Reads Redis first (cache + real-time features), falls back to DynamoDB-local.
- [ ] Defaults policy: missing feature → registry default, flagged as `missing`. Nulls never returned.
- [ ] OpenAPI schema committed to the repo; a test fails if the running service's schema drifts from the committed one (breaking-change tripwire).
- [ ] Latency histogram in `/metrics`; target p99 ≤ 10ms locally.

**Acceptance criteria:**
- [ ] 1. Request mixing batch, real-time, and nonexistent-entity features returns correct values + correct freshness_status for each, defaults substituted where needed.
- [ ] 2. Schema-drift test passes; deliberately changing a response field makes it fail.
- [ ] 3. **Parity test (required, in `tests/`):** for 20 sampled users, features served online equal features computed offline for the same date (within float tolerance).
- [ ] 4. p99 ≤ 10ms at 100 concurrent requests (locust or simple asyncio load script).

---

## Phase 4 — Ranking: training pipeline, model registry, scorer

**Goal:** a reproducible pCTR training loop with versioned, promotable model artifacts.

**Build:**
- [ ] `ranking/train.py`: builds training data from historical impressions via **point-in-time joins** against the offline store (features as of impression time — never current values); trains a pCTR model; outputs a versioned artifact directory `models/pctr/v{N}/` containing model file, feature list (pinned to registry names), training-data date range, and an eval report (AUC + calibration curve on a time-split holdout).
- [ ] **Model artifact contract (decomposability):** every artifact is loadable as an object exposing `predict(feature_dict) -> float` plus `feature_names()`. The scorer speaks only this interface — it never imports an ML library directly. A model version is opaque: the algorithm, hyperparameters, and feature list are internal details recorded in the artifact's metadata (`training_config.json`), not part of its identity. Training runs take a config file (which happens to name an algorithm, among other settings); the output is simply the next version. The registry, scorer, promotion flow, and A/B arms deal only in versions — v3 might be logistic regression and v4 XGBoost with a different feature set, and nothing downstream knows or cares. Comparing any two versions is one operation: eval reports on the identical holdout offline, arms in the A/B online.
- [ ] Minimal model registry: `models/registry.json` mapping logical name → version → path + status (`candidate` | `live` | `retired`). `ranking/promote.py v{N}` flips live pointer; rollback = promote previous version.
- [ ] `ranking/scorer.py`: library loaded by the ad server; loads whatever version is `live`, refuses to load if the model's feature list contains names absent from the feature registry.
- [ ] Sanity eval: model's learned lifts directionally match the planted signal from Phase 0 (traveler × travel ads etc.).
- [ ] **Feature assembly contract (user/ad separation):** four feature classes with distinct handling — *user* features (fetched once per request), *ad* features (fetched per candidate; cacheable in-process since the catalog is small and refreshed on materialization), *context* features (request-scoped: hour of day, slot — computed inline, never stored), and *cross* features (`x_` prefix: pure functions of the other three, e.g. `x_user_ctr_in_ad_category`; never stored, never in the online store). Cross functions live in one shared module (`common/crosses.py`) imported by BOTH `train.py` and the scorer — a single implementation is the defense against training-serving skew in the join. A model's pinned feature list may reference all four classes; the assembly step resolves each by class.

**Acceptance criteria:**
- [ ] 1. `make train` is fully reproducible: same data + seed → same eval metrics.
- [ ] 2. Eval report generated per version; AUC beats a popularity-only baseline; planted-signal check passes.
- [ ] 3. Leakage test: a deliberately leaked feature (post-click information) added in a test raises AUC suspiciously — test documents the detection, then asserts the production feature list excludes it.
- [ ] 4. Promote v2, roll back to v1 — scorer follows the live pointer without code changes.
- [ ] 5. Opacity proof: train two versions from configs that happen to use different algorithms and feature lists; both flow through the identical registry → promote → score → A/B path with zero code changes anywhere downstream, and the scorer module imports no ML library. Nothing outside the artifact directory reveals which algorithm a version uses.
- [ ] 6. Cross-parity test: for sampled (user, ad) pairs, cross features computed in the training path equal those computed in the serving path for the same inputs (same shared module, verified end to end — extends the Phase 3 parity test to derived features).

---

## Phase 5 — Ad server: retrieval, auction, pacing, yield, fallbacks

**Goal:** the centerpiece. A serving endpoint with an explicit SLO, a two-stage pipeline, mixed demand types, pacing state, an external bidder on a timeout, and an observable degradation ladder.

**Build:**
- [ ] **Written SLO first** (in `adserver/README.md` before code): p99 ≤ 100ms, availability 99.9%, defined degraded modes.
- [ ] `POST /serve` {user_id, session_id, slot}:
  1. *Candidate retrieval:* filter eligible campaigns (targeting rules, flight dates, budget/goal remaining) — cheap, no model. **Audience routing rule:** audiences participate here as *eligibility only, and only when purchased* — a campaign that targeted an audience is ineligible for users outside it; a campaign that didn't is unaffected by audience membership. Audiences must never act as system-imposed relevance filters (cliff edges, self-reinforcing data starvation, thinner auctions); relevance flows through scoring. A required code comment at the retrieval filter states this rule and why.
  2. *Feature fetch* from feature service with a 20ms budget.
  3. *Scoring:* pCTR from live model; auction candidates ranked by bid × pCTR (eCPM). Audience *affinity* (as opposed to purchased eligibility) enters here as cross features (e.g. `x_user_in_audience_matching_ad_category`), available to model configs like any other feature — the model learns how much membership should shift scores, including when it shouldn't.
  4. *External demand:* call `bidder_stub/` (configurable latency distribution + failure rate) with a hard 30ms timeout; a returned bid competes in the auction; timeout → internal demand only, logged.
  5. *Yield arbitration:* guaranteed campaigns paced toward impression goals over their flight (simple linear pacing: behind schedule → guaranteed wins the slot; ahead → auction competes). Spend/delivery counters live in Redis, decremented at serve time.
  6. *Degradation ladder* (each rung logged when fired): real-time features stale/missing → serve on batch only; feature service timeout → cached popularity ranking; model failure → house ad. Never a 500 to the caller.
- [ ] **Decision log = system of record:** one JSON line per request: request_id, ts, experiment arm, candidate set, **audience eligibility outcomes (which campaigns were excluded by which audience, with definition_version)**, per-candidate features + freshness + scores, external bid presence/outcome, winner, price, fallback rung. Written locally, loadable into DuckDB.
- [ ] **A/B assignment:** hash(user_id, salt) → arm; arm pins model version; assignment logged.
- [ ] Per-stage latency instrumentation in `/metrics` (retrieval / features / scoring / bidder / total).

**Key decisions locked:** pacing counters are best-effort (no transactions) — a required test demonstrates the concurrency flaw (two parallel requests both decrement the last budget dollar) and the README explains what production systems do about it. This is a feature of the project, not a bug.

**Acceptance criteria:**
- [ ] 1. Load test at 50 RPS: p99 ≤ 100ms; per-stage latencies visible.
- [ ] 2. **Failure-mode tests (required):** (a) stop Redis → popularity fallback serves, rung logged; (b) bidder latency forced above timeout → auction proceeds internally, timeout logged; (c) model artifact removed → house ad, rung logged. No request returns 500 in any scenario.
- [ ] 3. Guaranteed-delivery test: a campaign with a 1,000-impression goal over a simulated 10-day flight ends within ±10% of goal under steady traffic, and wins arbitration when behind schedule.
- [ ] 4. Concurrency test demonstrating the pacing overshoot; documented.
- [ ] 5. Two model versions live under A/B; assignment is deterministic per user and logged; arms receive ~50/50 traffic over 1,000 requests.
- [ ] 6. End-to-end freshness test: emit `destination_entered{category: travel}` → within 3 seconds, `/serve` for that user reflects the real-time feature in the logged feature set (and, given planted signal, shifts scores toward travel ads).
- [ ] 7. Audience test: a campaign targeting `frequent_airport_travelers` never serves to non-members (asserted over 1,000 replayed requests, exclusions visible in the decision log with definition_version); the same user still receives non-targeted campaigns from other categories — proving audiences gate eligibility, not relevance.

---

## Phase 6 — Measurement loop: reconciliation, retraining, experiment readout

**Goal:** close the loop — the decision log feeds measurement and the next model.

**Build:**
- [ ] `ops/reconcile.py`: batch job comparing decision-log impressions against pacing counters and campaign delivery; reports discrepancies (there will be some, per Phase 5's known flaw) with tolerances.
- [ ] Retraining path: `make retrain` builds training data from decision logs (not the original synthetic history), producing a candidate model version with an eval report comparing it to live.
- [ ] Experiment readout: small notebook/script computing per-arm CTR with confidence intervals from the decision log, joined on logged assignment. README note (not code) on why observational CTR comparisons across arms are trustworthy here (randomized assignment) and what would break trust (assignment drift, logging loss) — plus a paragraph mapping this to incrementality/lift measurement for brand campaigns, which is out of build scope.

**Acceptance criteria:**
- [ ] 1. Reconciliation runs and quantifies the serve-vs-counter discrepancy; report readable.
- [ ] 2. `make retrain` produces a promotable candidate trained purely on logged serving decisions.
- [ ] 3. Readout script yields per-arm CTR + CIs; a planted model-quality difference between arms is detectable at n=5,000 requests.

---

## Phase 7 — Debug UI + ops dashboard

**Goal:** two panes of glass — one for the request path, one for the system.

**Build:**
- [ ] `ui/` (Streamlit): Tab 1 "Rider": the full-featured successor to the Phase 2 mini page — pick a user, enter a destination (fires real session events through the same publish endpoint), see the served ad plus a debug panel — candidates, features (with freshness badges), scores, arbitration outcome, fallback rung, latency by stage. Tab 2 "Ops": request rate, p99 by stage, fallback-rung counts, consumer lag, spend/delivery by campaign, experiment arm split — polled from the services' `/metrics` and the decision log.

**Acceptance criteria:**
- [ ] 1. Typing a travel destination visibly changes the served ad / scores for a traveler-primed user within seconds.
- [ ] 2. Running any Phase 5 failure scenario is visible on the Ops tab (fallback counts rise, stage latency shifts) without reading logs.

---

## Phase 8 (optional) — One slice on real AWS

**Goal:** texture of real deployment without letting cloud ops dominate.

**Build:**
- [ ] Push the ad server container to ECR; run on ECS Fargate; point at a real DynamoDB table (features loaded by the Phase 1 job run locally with AWS credentials); CloudWatch for logs; $20 budget alarm on the account. Everything else stays local/stubbed.

**Acceptance criteria:**
- [ ] 1. A `curl` against the Fargate endpoint serves an ad using DynamoDB-hosted batch features.
- [ ] 2. Teardown script (`make aws-down`) removes all billable resources.
- [ ] 3. Total spend stays under the alarm.

---

## Runbook: adding a new product event → new real-time feature

The canonical extension path, end to end. Each step is a small, independent change; serving is never at risk at any step.

- [ ] 1. **Observe** (optional but the realistic starting point): the new event starts arriving and shows up in the consumer's `unknown_events_by_type` metric. Nothing breaks; the counter is your signal a new signal exists.
- [ ] 2. **Schema:** add the event type + payload fields to the event schema in `common/` (one commit; producer and mini page can now emit it, replayer can synthesize it).
- [ ] 3. **Registry:** declare the feature(s) it will produce in `common/registry.yaml` — name, entity, dtype, window, freshness SLA (seconds), TTL, default, owner.
- [ ] 4. **Handler:** add one module in `stream_features/handlers/` implementing `event_type()`, `outputs()`, `update(event, state)`. No edits to the consumer core.
- [ ] 5. **Verify precisely:** fire one event from the mini page → confirm the feature appears in Redis and is returned by the feature service with `fresh` status. Then fire nothing and confirm TTL expiry → `missing` + default.
- [ ] 6. **Verify at volume:** extend the replayer's event mix to include the new type; run the standard replay and check consumer lag and the feature's write rate in `/metrics`.
- [ ] 7. **Test:** one handler unit test + add the event to the Phase 2 integration replay fixture.
- [ ] 8. **Done — and deliberately stop here.** The live model ignores the feature (its feature list is pinned). It now accumulates in the online store and decision logs.
- [ ] 9. **Adopt (separate, later, a modeling decision not an engineering one):** add the feature to a new training config → next model version trains with it → offline eval vs live on the identical holdout → A/B arm → promote if it wins.

Steps 2–4 are the whole engineering change: schema, registry entry, handler. Everything else is verification or modeling. If a change ever requires touching the consumer core, feature service, scorer, or ad server, the seams have been violated — treat that as a design bug.

(The batch analog is the same shape minus the event steps: registry entry → one job module → `make features` → verify via feature service → adopt via training config.)

---

## Suggested pacing

Phases 0–1 in one sitting; 2–3 the next; 4 one sitting; 5 is the big one (two to three sittings — resist letting Claude Code build it in one shot; review the SLO/ladder design before implementation); 6–7 one sitting each; 8 a weekend if appetite remains.

## Trim order if scope must shrink

Cut in this order: Phase 8 → Ops dashboard tab (read logs manually) → reconciliation job → model registry (fall back to file naming). Never cut: failure-mode tests, decision logging, the kill test, the parity test, guaranteed-vs-auction arbitration.
