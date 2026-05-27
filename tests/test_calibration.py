import math
from src.geo.calibration import sigma_calibration, WARN_BAND


def _pairs(sigma, dists):
    return [{"distance_m": d, "airbnb_sigma": sigma} for d in dists]


def test_distinct_sigma_distinct_buckets_and_predicted():
    recs = _pairs(106.4, [99, 99, 99]) + _pairs(350.0, [300, 300])
    out = sigma_calibration(recs, geo_sigma=25.0, max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=2)
    sig = {b["airbnb_sigma"]: b for b in out["buckets"]}
    assert set(sig) == {106.4, 350.0}                       # one bucket per σ, not lumped
    assert sig[106.4]["n"] == 3 and sig[350.0]["n"] == 2
    assert sig[106.4]["predicted_m"] == round(math.hypot(106.4, 25.0), 1)
    assert sig[106.4]["measured_median_m"] == 99.0
    assert sig[106.4]["ratio"] == round(99.0 / math.hypot(106.4, 25.0), 2)


def test_max_dist_excludes_outliers():
    out = sigma_calibration(_pairs(100.0, [100, 100, 5000]), geo_sigma=25.0,
                            max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=1)
    assert out["buckets"][0]["n"] == 2                      # the 5000 m pair dropped
    assert out["buckets"][0]["measured_median_m"] == 100.0


def test_out_of_band_warns_only_when_enough_n():
    big = sigma_calibration(_pairs(100.0, [400] * 30), geo_sigma=25.0,
                            max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=30)
    assert big["buckets"][0]["ratio"] > 1.4 and big["buckets"][0]["warned"] is True
    small = sigma_calibration(_pairs(100.0, [400] * 5), geo_sigma=25.0,
                              max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=30)
    assert small["buckets"][0]["ratio"] > 1.4 and small["buckets"][0]["warned"] is False


def test_empty_input():
    out = sigma_calibration([], geo_sigma=25.0, max_dist_m=1000.0, warn_band=(0.6, 1.4))
    assert out["buckets"] == [] and out["warn_band"] == [0.6, 1.4]


def test_default_warn_band_applied_and_listified():
    # WARN_BAND is used when warn_band is omitted, and is emitted as a list (JSON-safe).
    out = sigma_calibration(_pairs(100.0, [100]), geo_sigma=25.0, max_dist_m=1000.0)
    assert WARN_BAND == (0.6, 1.4)
    assert out["warn_band"] == list(WARN_BAND)
