"""Planted segment x category (x time-of-day) click-lift table.

Single source of truth for the synthetic signal datagen plants into
events.parquet. datagen/README.md and the repo README render this table
verbatim so the docs can never drift from the generator.
"""

from __future__ import annotations

SEGMENTS = ["commuter", "traveler", "nightlife", "foodie", "shopper", "homebody", "general"]

CATEGORIES = ["food", "retail", "entertainment", "travel", "transit"]

BASE_CTR = 0.03
MAX_CTR = 0.6

# Night window (inclusive of 20-23 and 0-4).
NIGHT_HOURS = set(range(20, 24)) | set(range(0, 5))

# (segment, category, condition) -> lift multiplier. condition is "any",
# "night", or "day". Unlisted (segment, category) pairs default to 1.0x.
LIFT_TABLE: list[tuple[str, str, str, float]] = [
    ("commuter", "transit", "any", 3.0),
    ("commuter", "retail", "any", 1.3),
    ("traveler", "travel", "any", 3.0),
    ("traveler", "retail", "any", 1.2),
    ("nightlife", "entertainment", "night", 3.5),
    ("nightlife", "food", "night", 2.5),
    ("nightlife", "entertainment", "day", 1.0),
    ("foodie", "food", "any", 3.0),
    ("shopper", "retail", "any", 3.0),
    ("shopper", "entertainment", "any", 1.2),
    ("homebody", "*", "any", 0.3),
    ("general", "*", "any", 1.0),
]


def lift_for(segment: str, category: str, hour_of_day: int) -> float:
    """Return the click-probability multiplier for a (segment, category, hour)."""
    condition = "night" if hour_of_day in NIGHT_HOURS else "day"

    best: float | None = None
    for seg, cat, cond, mult in LIFT_TABLE:
        if seg != segment:
            continue
        if cat != category and cat != "*":
            continue
        if cond == "any" or cond == condition:
            # More specific (exact category, exact condition) wins over
            # wildcard category or "any" condition.
            specificity = (cat != "*", cond != "any")
            if best is None or specificity >= best[0]:
                best = (specificity, mult)
    if best is not None:
        return best[1]
    return 1.0


def click_probability(segment: str, category: str, hour_of_day: int) -> float:
    p = BASE_CTR * lift_for(segment, category, hour_of_day)
    return min(p, MAX_CTR)
