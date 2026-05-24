from src.geo.fusion import Observation, fuse_observations, position_confidence


def test_precise_observation_dominates_fuzzed_one():
    obs = [
        Observation("booking_1", 44.4300, 26.1000, 25.0, "geocoded"),   # precise
        Observation("airbnb_2", 44.4330, 26.1000, 100.0, "scraped"),    # fuzzed ~330 m N
    ]
    fused = fuse_observations(obs)
    # Result should sit very close to the precise point, not the midpoint.
    assert abs(fused.latitude - 44.4300) < 0.0005
    assert fused.sigma_m < 25.0  # fusing reduces uncertainty
    assert fused.dominant_listing_id == "booking_1"


def test_two_equal_approximate_points_reduce_sigma():
    obs = [
        Observation("a", 44.4300, 26.1000, 100.0, "scraped"),
        Observation("b", 44.4300, 26.1000, 100.0, "scraped"),
    ]
    fused = fuse_observations(obs)
    assert abs(fused.sigma_m - 70.7) < 1.0  # 100/sqrt(2)


def test_outlier_rejected():
    obs = [
        Observation("a", 44.4300, 26.1000, 50.0, "geocoded"),
        Observation("b", 44.4302, 26.1001, 50.0, "scraped"),
        Observation("c", 45.0000, 27.0000, 100.0, "scraped"),  # >1 km outlier
    ]
    fused = fuse_observations(obs)
    assert fused.latitude < 44.45  # the outlier did not drag it north


def test_confidence_monotonic():
    assert position_confidence(20.0) > position_confidence(120.0)
    assert 0.0 <= position_confidence(500.0) <= 1.0


def test_fused_sigma_floored_at_min():
    # Eight non-independent tight observations would fuse to ~5.3 m (15/sqrt(8)),
    # but the floor caps the claimed deviation at the coordinate-rounding limit.
    obs = [Observation(f"x{i}", 44.4300, 26.1000, 15.0, "scraped") for i in range(8)]
    assert fuse_observations(obs).sigma_m == 10.0
    # A genuine two-source fuse stays above the floor (unaffected).
    two = fuse_observations([Observation("a", 44.43, 26.10, 15.0, "scraped"),
                             Observation("b", 44.43, 26.10, 25.0, "geocoded")])
    assert abs(two.sigma_m - 12.86) < 0.2   # 1/sqrt(1/225 + 1/625) ≈ 12.86
    # the floor is overridable
    assert fuse_observations(obs, min_sigma_m=0.0).sigma_m < 6.0
