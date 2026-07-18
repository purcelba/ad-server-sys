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

The Phase 0 synthetic catalog (`datagen/`) produces two entity tables.
Later phases only ever *add* columns/files (e.g. Phase 1's
`audience_memberships`) — they don't redefine these.

### `users.parquet`
| column | type | notes |
|---|---|---|
| `user_id` | str | unique, e.g. `u_0001` |
| `segment` | str (enum) | primary behavioral segment — exact set + planted lift factors defined in `datagen/README.md` (Phase 0 AC #3); examples from the spec: `commuter`, `traveler`, `nightlife` |
| `home_metro` | str | synthetic geo bucket, used for targeting/geo features |
| `created_at` | date | account creation date, within the 30-day synthetic history window |

### `campaigns.parquet` (ads)
| column | type | notes |
|---|---|---|
| `campaign_id` | str | unique, e.g. `c_0001` |
| `advertiser_name` | str | synthetic advertiser label |
| `category` | str (enum) | one of `food`, `retail`, `entertainment`, `travel`, `transit` (per spec Phase 0) |
| `demand_type` | str (enum) | `auction` \| `guaranteed` |
| `bid` | float, nullable | set only when `demand_type == auction` |
| `budget` | float, nullable | set only when `demand_type == auction` |
| `impression_goal` | int, nullable | set only when `demand_type == guaranteed` |
| `flight_start` | date | required for both demand types |
| `flight_end` | date | required for both demand types |
| `status` | str (enum) | `active` \| `paused` \| `ended` |

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
