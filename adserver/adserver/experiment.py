"""A/B assignment: hash(user_id, salt) -> arm. Deterministic per user (the
same user always lands in the same arm for a given salt) so repeated
requests don't flicker between model versions, and reproducible for
testing without needing to store assignment state anywhere.
"""

from __future__ import annotations

import hashlib

DEFAULT_SALT = "phase5-ab-v1"

# Arm -> pinned model version. Both v1 and v2 already exist from Phase 4.
ARM_VERSIONS = {"control": "v1", "treatment": "v2"}


def assign_arm(user_id: str, salt: str = DEFAULT_SALT) -> str:
    digest = hashlib.sha256(f"{salt}:{user_id}".encode()).hexdigest()
    return "control" if int(digest, 16) % 2 == 0 else "treatment"
