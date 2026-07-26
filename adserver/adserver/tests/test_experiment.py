import uuid

from adserver.adserver.experiment import ARM_VERSIONS, assign_arm


def test_assignment_is_deterministic_for_the_same_user_and_salt():
    user_id = f"u_{uuid.uuid4().hex[:8]}"
    assert assign_arm(user_id) == assign_arm(user_id)


def test_assignment_only_returns_known_arms():
    for _ in range(100):
        user_id = f"u_{uuid.uuid4().hex[:8]}"
        assert assign_arm(user_id) in ARM_VERSIONS


def test_assignment_is_roughly_50_50_over_many_users():
    counts = {"control": 0, "treatment": 0}
    for i in range(2000):
        counts[assign_arm(f"u_{i}")] += 1
    ratio = counts["control"] / sum(counts.values())
    assert 0.4 < ratio < 0.6


def test_salt_is_actually_part_of_the_hash_input():
    """Across many users, a different salt must flip at least some
    assignments — proves salt isn't silently ignored, without pinning to
    any single user's specific outcome (which could coincidentally match)."""
    flipped = sum(
        1
        for i in range(200)
        if assign_arm(f"u_{i}") != assign_arm(f"u_{i}", salt="a-completely-different-salt")
    )
    assert flipped > 0
