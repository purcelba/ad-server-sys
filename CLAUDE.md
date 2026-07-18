# Toy Ad Server — CLAUDE.md

This project follows `phases.md`. Read that file for the phase you're
working on. This file holds the conventions and standing instructions that
apply across every phase.

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

## Why Polars, not pandas

"Polars for transforms" is a locked Phase 0 decision (`phases.md`); the
reasoning, for when it's not obvious why a given transform is written in
Polars idioms rather than pandas:

- **Performance/memory**, mainly from being Rust-based on top of Apache
  Arrow's columnar memory format, vs. pandas' older NumPy-block internals —
  matters for the windowed aggregations `batch_features/` (Phase 1)
  computes over parquet-partitioned history (e.g. `user_ctr_by_category_30d`).
- **Lazy evaluation** (`pl.scan_parquet(...).filter(...).group_by(...).collect()`)
  — Polars builds and optimizes a query plan (predicate/projection
  pushdown) before touching data; pandas is eager only. This is what makes
  the point-in-time reads Phase 1/4 need (filter to a date range before
  materializing anything) cheap.
- **Native Arrow/Parquet interop.** `datagen/`, `batch_features/`, and
  DuckDB all read/write Parquet — staying in the Arrow ecosystem avoids
  conversion overhead and the dtype surprises pandas' non-Arrow default
  dtypes can introduce.
- **Real nullable types throughout**, vs. pandas' historical looseness
  around `NaN`/`None`/`dtype=object` for nullable ints and strings — this
  project leans on explicit nullability rules (e.g. `campaigns.parquet`'s
  `bid`/`budget`/`impression_goal`, `events.parquet`'s `click_id`), where
  that looseness would be a real bug source.
- **Clean interop with DuckDB** (see "DynamoDB vs. DuckDB" below) — both
  are Arrow-native, so `batch_features/` and `ranking/train.py` can move
  data between Polars transforms and DuckDB SQL cheaply.

**Coming from pandas.** Concepts transfer (still rows/typed columns,
filter/group/join/sort as the core verbs, `.head()`/`.shape`/Parquet I/O
feel the same), but expect some syntax relearning: no `.loc`/`.iloc`/boolean
`df[mask]` indexing — everything goes through `.filter(pl.col(...) == ...)`
expressions; no index at all (no `.reset_index()`, no index-alignment
footguns); method chaining is the idiom, not a style choice (see
`adserver/datagen/eda.py::_ctr_pivot` for a real example); `.group_by().agg()`
takes expressions, not string names or a dict of lists; lazy (`pl.scan_parquet`
+ `.collect()`) vs. eager (`pl.read_parquet`) is an explicit choice, not
automatic; nulls are always `None`, never a silent int→float `NaN` upcast;
`.pivot()` takes `on=`/`index=`/`values=` rather than pandas' argument names.

**Downsides, for balance:** smaller ecosystem (some libraries — plotting,
older stats/finance packages — still expect pandas or NumPy, requiring an
explicit `.to_numpy()`/`.to_pandas()` at that boundary); less
Stack-Overflow/tutorial coverage than pandas' ~15-year head start; a less
stable API history (real breaking changes across versions, e.g.
`groupby`→`group_by`, `pivot`'s renamed args — pin versions carefully, which
`uv.lock` already does); no index-based auto-alignment (occasionally more
verbose, though more predictable); smaller pool of pandas-only scientific
extensions.

**Modeling interop (Phase 4 concern, resolved):** not expected to be a real
issue. The model artifact contract's `predict(feature_dict) -> float` means
no DataFrame exists at serving time at all — the only DataFrame-to-modeling
boundary is inside `train.py` building the training matrix, and neither
`scikit-learn` nor `xgboost`'s sklearn API requires pandas; both fit
directly on `polars_df.to_numpy()`. If a library genuinely needs pandas
(e.g. some explainability/plotting tooling expecting pandas `Categorical`
dtype or column-name-aware output), `polars_df.to_pandas()` is a fast
Arrow-backed bridge, not a slow re-serialization — an occasional one-line
escape hatch at one boundary, not a recurring tax through the codebase.

## Docker Compose services

Three infra containers, brought up by `make up` (`docker-compose.yml`).
Everything else in the project is a plain Python process — no Kubernetes,
no managed cloud services, per the "explicitly out of scope" list above.
Versions are pinned (not `latest`), consistent with the project's
determinism/reproducibility ethos.

- **`redpanda`** (`docker.redpanda.com/redpandadata/redpanda:v24.2.4`) — the
  event broker. Speaks the Kafka wire protocol, so `stream_features/`
  (Phase 2) can use a standard Kafka client without running actual Kafka +
  ZooKeeper. Chosen over real Kafka specifically for local ops simplicity:
  it's a single binary/container with no separate coordination service,
  while still being protocol-compatible enough that the concepts (event
  time vs. processing time, consumer lag, at-least-once delivery) transfer
  directly. *Production analog:* Kinesis (or Kafka) + Flink — this project
  intentionally uses a plain Python consumer instead of Flink, documented
  as a locked decision in `phases.md` Phase 2.
- **`dynamodb-local`** (`amazon/dynamodb-local:2.5.4`) — the durable
  *online* feature store (store of record for serving). Batch-computed
  features (Phase 1) get materialized *into* it with `computed_at`
  timestamps; the feature service (Phase 3) falls back to it when Redis
  misses. Chosen because it's the actual DynamoDB API running locally —
  no emulation gap between local dev and Phase 8's optional real-AWS
  slice, which points the same code at a real DynamoDB table. *Production
  analog:* DynamoDB itself.
- **`redis`** (`redis:7.4-alpine`) — the low-latency, in-memory front of
  the online feature store, and, later, pacing/delivery counters (Phase
  5). Real-time features from `stream_features/` (Phase 2) are written
  here with TTLs matching each feature's freshness SLA; the feature
  service reads Redis first, falling back to DynamoDB-local. A small,
  well-understood in-memory store fits the sub-10ms p99 target (Phase 3
  AC4) and the sub-100ms serving SLO (Phase 5) without needing a heavier
  caching layer. *Production analog:* Redis (or an equivalent in-memory
  store) fronting a feature store — this is a case where the local
  stand-in and the production component are literally the same
  technology.

**DynamoDB vs. DuckDB — not the same thing, despite the name.** DynamoDB
(-local) is a distributed key-value store: point lookups by entity ID,
low-latency, the *online serving-side* store described above. DuckDB is a
different, unrelated technology — an embedded, in-process analytical (OLAP)
query engine, more like SQLite but columnar, with no server of its own. It
never appears in `docker-compose.yml`; it's a Python library dependency
used by `batch_features/` (Phase 1) and `ranking/train.py` (Phase 4) to run
SQL directly over the date-partitioned Parquet files in the *offline*
store — in particular, point-in-time queries ("features as of day 15")
that training needs to avoid leaking future values into training examples.
Same underlying data flows one direction: Parquet (queried via DuckDB) →
materialized into DynamoDB-local (queried via point lookup) for serving.

| | DynamoDB(-local) | DuckDB |
|---|---|---|
| Role | online, durable feature store | query engine over the offline Parquet store |
| Access pattern | point lookup by entity ID | analytical SQL / point-in-time scans |
| Used by | `feature_service/` (Phase 3) | `batch_features/` (Phase 1), `ranking/train.py` (Phase 4) |
| Runs as | a `docker-compose.yml` container | an embedded library, no container |

## Real-time state vs. long-term aggregates — why separate stores

A recurring question when adding a new signal: does it belong in the
real-time path (Redis, TTL'd) or the batch path (Parquet/DuckDB offline →
DynamoDB-local online)? The two have fundamentally different requirements,
which is why production systems — and this project — split them rather
than using one store for both:

| | Immediate/real-time state | Long-term/historical aggregate |
|---|---|---|
| Freshness need | seconds | hours (daily batch is often fine) |
| Write pattern | continuous stream of small per-event writes | periodic bulk recompute/upsert over a window |
| Read pattern | point lookup, "give me the current value" | point lookup for serving, but *computed* via range/aggregation queries |
| Durability | ephemeral — should expire (TTL) when no longer relevant | must persist; often needs point-in-time query ("as of date D") |
| Failure blast radius | a lagging pipeline should degrade gracefully, not take serving down | independent pipeline, independent failure mode |

This project ends up with three tiers, not two, because "online" splits
further by latency need:
1. **Offline analytical store** (Parquet + DuckDB) — the historical
   record, point-in-time queryable, used for training (Phase 4).
2. **Online durable store** (DynamoDB-local) — the latest *materialized*
   value of batch-computed features, one point lookup away (Phase 1→3).
3. **Online real-time cache** (Redis, TTL'd) — "what's happening right
   now," written continuously as events arrive (Phase 2→3).

This split is what makes graceful degradation possible: Phase 5's
degradation ladder treats "real-time features stale/missing → fall back to
batch-only" as a first-class failure mode specifically *because* the two
paths are decoupled. If one store served both, a stream-processing outage
would take the whole feature down instead of quietly aging out only the
real-time-only piece.

**Concrete precedent — `user_rides_per_week`:** this Phase 1 feature is a
rolling aggregate over time, not a point-in-time signal, so it's a *batch*
concept by definition, computed from `rides.parquet` (added to `datagen/`
during Phase 1 planning — see `PROGRESS.md`'s Phase 0 entry for why this
counts as a flagged amendment to an already-tagged phase). A hypothetical
future "user is currently on a ride" feature would be a *different,
real-time* feature via `stream_features/` → Redis (Phase 2's territory),
not an alternate implementation of the same signal — the two aren't
interchangeable just because they're both "about rides."

## Makefile targets

`up`/`down` manage infra containers, `test` proves correctness, `demo`/`eda`
run the actual generator and show you what it produces. Only `eda` depends
on another target (`demo`).

- **`make up`** — `docker compose up -d`, then polls `docker compose ps`
  every 2s (up to 60 tries) until all 3 services report `"Health":
  "healthy"`, printing "All infra healthy." On timeout it prints an error
  plus a `docker compose ps` dump and exits non-zero. Infra only — no
  Python code runs. This is what `test_infra_healthy` (Phase 0 AC1) checks
  against.
- **`make down`** — `docker compose down -v`: stops containers and removes
  volumes, a clean slate.
- **`make test`** — `uv run pytest adserver/ -v`: runs the full test suite.
  Doesn't bring infra up itself — if infra isn't running,
  `test_infra_healthy` self-skips (checks `docker compose ps`, skips with a
  message rather than failing) while every other test still runs. CI calls
  `make up` first, then this.
- **`make demo`** — runs the datagen CLI directly (`uv run python -m
  adserver.datagen.cli --seed 42 --out data/`), generating
  `users.parquet`/`campaigns.parquet`/`events.parquet` into `data/`, then
  prints a `.head()` preview of each so output is eyeballable without
  writing a script. No infra dependency, no tests.
- **`make eda`** (depends on `demo`) — regenerates the data, then runs
  `adserver/datagen/eda.py` to write the CTR heatmaps (segment × category,
  segment × day/night bucket, plus the raw click-volume-by-hour
  diagnostic) to `data/eda/`.

## CI/CD

**CI:** `.github/workflows/ci.yml` runs on every push and PR. It installs
`uv`, runs `uv sync`, brings up the full Docker Compose infra (`make up`),
runs the whole test suite against it (`make test`), then tears infra down
(`make down`, `if: always()` so it runs even on failure). There's no
separate lint/build/typecheck job — `make test` is the single gate, kept
that way deliberately so "CI passing" and "the acceptance criteria pass"
mean the same thing.

Pushing `.github/workflows/*` requires a GitHub token with the `workflow`
OAuth scope (stricter than plain `repo`, since workflow files can execute
code with repo permissions on GitHub's infrastructure) — if a push is
rejected for this reason, the fix is `gh auth refresh -h github.com -s
workflow`, which needs the user to approve a device-code prompt in their
browser; Claude Code cannot approve this step on the user's behalf.

**CD:** none. This project has no automated deployment — it's local-scale
by design (`docker compose` + local Python processes), and Phase 8's
optional AWS slice is explicitly a manual, one-off exercise (`make
aws-down` is a manual teardown script, not part of any pipeline). If a
future phase's README claims otherwise, that's a spec deviation — flag it.

## Data schemas

The Phase 0 synthetic catalog (`datagen/`) produces three entity/event
tables, all locked as of the `phase-0` tag. Later phases only ever *add*
columns/files (e.g. Phase 1's `audience_memberships`) — they don't redefine
these. Full generation details (segment counts, campaign ranges, lift
table) live in `adserver/datagen/README.md`; this is the column contract.

### `users.parquet`
| column | type | notes |
|---|---|---|
| `user_id` | str | unique, e.g. `u_0001` |
| `segment` | str (enum) | one of `commuter`, `traveler`, `nightlife`, `foodie`, `shopper`, `homebody`, `general` — `homebody` (suppressed engagement) and `general` (no lift) are deliberate control groups; planted lift factors defined in `datagen/lifts.py` / rendered in `datagen/README.md` (Phase 0 AC #3) |
| `home_metro` | str | one of `san_francisco`, `new_york`, `chicago`, `austin`, `seattle` |
| `created_at` | date | account creation date, within the synthetic history window |

### `campaigns.parquet` (ads)
| column | type | notes |
|---|---|---|
| `campaign_id` | str | unique, e.g. `c_0001` |
| `advertiser_name` | str | synthetic advertiser label |
| `category` | str (enum) | one of `food`, `retail`, `entertainment`, `travel`, `transit` |
| `demand_type` | str (enum) | `auction` \| `guaranteed` |
| `bid` | float, nullable | non-null iff `demand_type == auction`; $0.50–$5.00 |
| `budget` | float, nullable | non-null iff `demand_type == auction`; $200–$2,000 |
| `impression_goal` | int, nullable | non-null iff `demand_type == guaranteed`; 500–5,000 |
| `flight_start` | date | required for both demand types |
| `flight_end` | date | required for both demand types |
| `status` | str (enum) | `active` \| `paused` \| `ended` |

### `events.parquet` (impressions/clicks)
| column | type | notes |
|---|---|---|
| `event_id` | str | unique, e.g. `e_00000001` |
| `event_type` | str (enum) | `impression` \| `click` |
| `user_id` | str | FK → users |
| `campaign_id` | str | FK → campaigns |
| `category` | str | denormalized from campaign at generation time — avoids joins in downstream feature jobs |
| `segment` | str | denormalized from user, same reason |
| `ts` | datetime (us) | event timestamp within the history window |
| `event_date` | date | derived from `ts` |
| `hour_of_day` | int 0–23 | denormalized from `ts` |
| `click_id` | str, nullable | for click rows only, FK → the impression's `event_id`; null on impression rows |

`audience_memberships` (on users) and audience *targeting* (on campaigns)
are Phase 1 additions (`audiences.yaml`) — not part of the Phase 0 catalog.
Don't add them to `datagen/` when building Phase 0.

## Per-phase loop

The discipline that makes this a learning project rather than a
code-generation project. This loop repeats every phase, not just at
kickoff:

1. **Fresh session per phase.** Each phase starts with `/clear` — long
   context degrades output, and every phase is designed to be
   self-contained given this file plus its section of `phases.md`.
   Expect a kickoff like: "Read phases.md Phase N. Plan before
   implementing."
2. **Plan mode, reviewed against locked decisions.** The plan gets checked
   against the phase's "Key decisions locked" before approval — this is
   where the human reviewer learns the most, catching a plan that quietly
   deviates from spec. Don't treat plan approval as a formality.
3. **Prove the acceptance criteria — don't assert them.** After
   implementing, run the acceptance criteria and show the passing output.
   "Should work" is not an acceptable substitute; the criteria are
   executable on purpose.
4. **Diffs get read before committing** — not every line, but the
   interfaces and the tests. "Explain this module to me as if I'm
   reviewing it" is a legitimate, expected request, not a distraction —
   answer it as a real design-review explanation, not a summary.
5. **Tag and log.** Once a phase's boxes are checked and the repo tagged
   (`phase-N`), append a short entry to `PROGRESS.md`: what was built,
   decisions made, deviations from spec. That file is the re-entry point
   after gaps between sessions — keep entries short and factual.

Two failure modes to actively resist:
- **Scope creep.** Don't build ahead into future phases even when the
  current phase makes them obvious — see "work only on the phase I name"
  below. Expect to be told no here occasionally; that's the mechanism
  working, not friction to route around.
- **Test-weakening.** When a test fails, diagnose before fixing. Default to
  "explain why this fails, then propose a fix," never a silent "make the
  test pass" that loosens the test itself — that destroys the acceptance
  criteria as a contract.

## Standing instructions for Claude Code

- **Work only on the phase I name.** Do not start, scaffold, or "get ahead" on later phases even if the current phase makes them obvious.
- **Before writing code, present a plan and wait for approval.** This applies at the start of every phase and to any significant redirection within a phase.
- **Write acceptance-criteria tests before or alongside implementation, never after.** If a phase's acceptance criteria aren't yet expressed as tests, write those tests first.
- **Commit per passing criterion**, with descriptive, conventional commit messages — one commit per acceptance criterion as it goes green, not one big commit at the end of a phase.
- **Never modify a completed phase's code without flagging it first.** A phase is complete once it's tagged (`phase-0`, `phase-1`, ...). If finishing the current phase requires touching an earlier phase's code, stop and say so explicitly before making the change.
- **Respect the seams.** Per the spec's extension runbook: adding a new feature should only ever touch the event schema, the feature registry, and a new handler module. If a change would require touching the consumer core, the scorer, the feature service, or the ad server itself to support what should be an isolated extension, **stop and say so** — that's a design bug, not a shortcut worth taking.
