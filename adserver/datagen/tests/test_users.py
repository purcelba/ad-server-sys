import numpy as np

from adserver.datagen.users import N_USERS, SEGMENT_COUNTS, generate_users


def test_user_count_and_schema():
    users = generate_users(np.random.default_rng(1))
    assert users.height == N_USERS == 50
    assert users.columns == ["user_id", "segment", "home_metro", "created_at"]
    assert users["user_id"].n_unique() == N_USERS


def test_segment_distribution_matches_declared_counts():
    users = generate_users(np.random.default_rng(1))
    counts = dict(users["segment"].value_counts().iter_rows())
    assert counts == SEGMENT_COUNTS
