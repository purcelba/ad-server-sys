# common

## What it does
Shared, governed contracts other components read but never bypass. So far:
the feature registry (`registry.yaml` + `registry.py`) — every feature any
job produces or any service serves must be declared here first: name,
entity (`user`|`ad`), dtype, description, aggregation, window,
freshness SLA, default value, owner. `load_registry()` validates the file
and raises a clear error naming the offending feature/field on any
malformed entry (missing field, invalid entity/dtype, unparseable
freshness SLA, duplicate name).

Also: named, versioned audiences (`audiences.yaml` + `audiences.py`) —
sellable segments defined as ANDed rules (`feature`/`op`/`value` triples)
over registry features, e.g. `frequent_airport_travelers` = segment ==
traveler AND user_ctr_by_category_30d.travel >= 0.05. Same governance
pattern as the registry: `load_audiences()` validates required fields, a
valid comparison op, and no duplicate names, raising a clear error naming
the offender. Changing a definition must bump `definition_version` —
`batch_features/jobs/audiences.py` logs every audience's version on every
run (`data/audience_versions.log`), so membership drift under an unchanged
name is always visible.

Per `CLAUDE.md`, this is the *only* code other components may import
across component boundaries — everything else communicates via HTTP, the
event stream, or the online store.

(Shared metrics helpers are also planned for this package within Phase 1;
not yet present as of this commit.)

## How to run and test it alone
```bash
uv run pytest adserver/common -v
```
No infra dependency — pure Python + a YAML file.

## Production analog
This is a lightweight, file-based stand-in for a real feature registry /
metadata catalog (e.g. Feast's registry, or an internal feature-platform
metadata service) — the same governance idea (a feature must be declared
before it can be computed or served) without the operational weight of a
hosted service, appropriate at this project's local scale.

## Ownership note
Under an end-to-end ads team model, the registry schema itself is
negotiable — plausibly owned by the ads team (they know what features
ranking needs) or by a central ML/data platform team (they own the
governance mechanism and its use across many teams' features). The
tradeoff: ads-team ownership moves faster for ads-specific features;
platform ownership gets consistency across teams but adds a
coordination step to ship a new feature.
