from __future__ import annotations

import math
from collections import defaultdict
from statistics import median

from ..dedup.deduplicator import haversine_distance

# Default acceptance band for the measured-median / predicted-RMS ratio.
# A correctly-calibrated ladder yields ~0.9 (a median compared to an RMS), so the
# band is centred on that, not on 1.0. Below lo -> priors too pessimistic; above
# hi -> measured error exceeds even the RMS prediction (priors too optimistic).
WARN_BAND = (0.6, 1.4)


def sigma_calibration(pair_records, *, geo_sigma, max_dist_m, warn_band=WARN_BAND, min_n=30):
    """Bucket geocoded-Booking cross-platform pairs by the Airbnb point's assigned
    σ and compare the measured median displacement to the predicted RMS.

    pair_records: iterable of {"distance_m": float, "airbnb_sigma": float}.
    Returns a JSON-serialisable dict:
      {"warn_band": [lo, hi], "min_n": int, "buckets": [
         {"airbnb_sigma", "n", "measured_median_m", "predicted_m", "ratio", "warned"}...]}
    Buckets are sorted by airbnb_sigma. `warned` is True only when n >= min_n AND
    the ratio is outside warn_band (small buckets are reported, never warned)."""
    lo, hi = warn_band
    by_sigma: dict[float, list[float]] = defaultdict(list)
    for r in pair_records:
        s = r.get("airbnb_sigma")
        d = r.get("distance_m")
        if s is None or d is None or d > max_dist_m:
            continue
        by_sigma[round(float(s), 1)].append(float(d))

    buckets = []
    for sigma in sorted(by_sigma):
        dists = by_sigma[sigma]
        predicted = math.hypot(sigma, geo_sigma)
        measured = float(median(dists))
        ratio = measured / predicted if predicted else 0.0
        warned = len(dists) >= min_n and not (lo <= ratio <= hi)
        buckets.append({
            "airbnb_sigma": sigma,
            "n": len(dists),
            "measured_median_m": round(measured, 1),
            "predicted_m": round(predicted, 1),
            "ratio": round(ratio, 2),
            "warned": warned,
        })
    return {"warn_band": [lo, hi], "min_n": min_n, "buckets": buckets}


def compute_offsets(cross_groups, members_by_key, by_id, geocoded_map, scraped_sigma):
    """For each cross-platform group, the Airbnb-pin <-> Booking-(geocoded-else-scraped)
    distances. Returns (offset_writes, calib_pairs):
      offset_writes: {listing_id: (offset_m, source)} — the group's median distance +
        "geocoded"/"scraped", written to EVERY member of the group.
      calib_pairs: [{"distance_m", "airbnb_sigma"}] — one per geocoded-Booking pair,
        the calibration input."""
    offset_writes: dict[str, tuple[float, str]] = {}
    calib_pairs: list[dict] = []
    for gid in cross_groups:
        members = members_by_key.get(gid, [])
        ab = [m for m in members if by_id[m]["platform"] == "airbnb"]
        bk = [m for m in members if by_id[m]["platform"] == "booking"]
        if not ab or not bk:
            continue
        dists: list[float] = []
        used_geocoded = False
        for a in ab:
            ar = by_id[a]
            a_sigma = scraped_sigma.get(a)
            for b in bk:
                if b in geocoded_map:
                    blat, blng, _ = geocoded_map[b]
                    is_geo = True
                else:
                    br = by_id[b]
                    blat, blng = br["latitude"], br["longitude"]
                    is_geo = False
                d = haversine_distance(ar["latitude"], ar["longitude"], blat, blng)
                dists.append(d)
                if is_geo:
                    used_geocoded = True
                    if a_sigma is not None:
                        calib_pairs.append({"distance_m": d, "airbnb_sigma": a_sigma})
        if not dists:
            continue
        value = (round(float(median(dists)), 1), "geocoded" if used_geocoded else "scraped")
        for m in members:
            offset_writes[m] = value
    return offset_writes, calib_pairs
