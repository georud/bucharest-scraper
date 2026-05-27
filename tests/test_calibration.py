import math
from src.geo.calibration import sigma_calibration, compute_offsets, WARN_BAND


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


def _row(lid, platform, lat, lng):
    return {"id": lid, "platform": platform, "latitude": lat, "longitude": lng}


def test_compute_offsets_geocoded_pair():
    by_id = {"ab": _row("ab", "airbnb", 44.4300, 26.1000),
             "bk": _row("bk", "booking", 44.4300, 26.1000)}
    members_by_key = {"g1": ["ab", "bk"]}
    geocoded_map = {"bk": (44.4309, 26.1000, "addr")}   # ~100 m north of the pin
    scraped_sigma = {"ab": 106.4, "bk": 50.0}
    offsets, calib = compute_offsets({"g1"}, members_by_key, by_id, geocoded_map, scraped_sigma)
    # both members carry the same offset; source is geocoded
    assert offsets["ab"][1] == "geocoded" and offsets["bk"][1] == "geocoded"
    assert 95 <= offsets["ab"][0] <= 105 and offsets["ab"][0] == offsets["bk"][0]
    # calibration pair carries the Airbnb point's sigma + the geocoded distance
    assert len(calib) == 1 and calib[0]["airbnb_sigma"] == 106.4
    assert abs(calib[0]["distance_m"] - offsets["ab"][0]) < 1.0


def test_compute_offsets_scraped_fallback_and_no_calib():
    by_id = {"ab": _row("ab", "airbnb", 44.4300, 26.1000),
             "bk": _row("bk", "booking", 44.4305, 26.1000)}
    members_by_key = {"g1": ["ab", "bk"]}
    offsets, calib = compute_offsets({"g1"}, members_by_key, by_id, {}, {"ab": 15.0})
    assert offsets["ab"][1] == "scraped"
    assert calib == []                                  # no geocoded pair -> no calibration input


def test_compute_offsets_skips_single_platform_group():
    by_id = {"ab": _row("ab", "airbnb", 44.43, 26.10), "ab2": _row("ab2", "airbnb", 44.43, 26.10)}
    offsets, calib = compute_offsets({"g1"}, {"g1": ["ab", "ab2"]}, by_id, {}, {"ab": 15.0, "ab2": 15.0})
    assert offsets == {} and calib == []


def test_compute_offsets_multi_member_group():
    by_id = {"ab1": _row("ab1", "airbnb", 44.4300, 26.1000),
             "ab2": _row("ab2", "airbnb", 44.4310, 26.1000),   # ~111 m from the booking
             "bk": _row("bk", "booking", 44.4300, 26.1000)}
    members_by_key = {"g1": ["ab1", "ab2", "bk"]}
    geocoded_map = {"bk": (44.4300, 26.1000, "addr")}
    scraped_sigma = {"ab1": 15.0, "ab2": 106.4, "bk": 50.0}
    offsets, calib = compute_offsets({"g1"}, members_by_key, by_id, geocoded_map, scraped_sigma)
    # all three members carry the same group offset = median of the 2 pair distances (~0 and ~111 m)
    assert offsets["ab1"][0] == offsets["ab2"][0] == offsets["bk"][0]
    assert 50 <= offsets["ab1"][0] <= 62
    # one calibration pair per (airbnb, booking) pair, each with its own airbnb sigma
    assert len(calib) == 2
    assert {p["airbnb_sigma"] for p in calib} == {15.0, 106.4}
