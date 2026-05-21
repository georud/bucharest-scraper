from __future__ import annotations

import logging
from collections import Counter, defaultdict

from ..dedup.deduplicator import haversine_distance
from ..dedup.operators import assign_operator_ids
from ..dedup.property_groups import assign_property_groups
from ..dedup.validate import dedup_metrics
from .fusion import Observation, fuse_observations, position_confidence
from .geocode import Geocoder
from .precision import (
    classify_scraped_precision, extract_booking_address,
    SIGMA_GEOCODED,
)

logger = logging.getLogger(__name__)


def run_curation(db, config=None, fetch_fn=None, backfill_rows=None) -> dict:
    """Run the full geo/dedup curation stage on the DB. Returns dedup metrics.

    Steps: reset -> operators -> property groups -> precision observations ->
    geocode -> fusion -> verification. `fetch_fn` injects the geocoder HTTP for
    tests; `backfill_rows` is an optional list of historical observation tuples
    (listing_id, lat, lng, sigma_m, capture_date, platform) from a prior DB."""
    rows = db.get_listings_for_curation()
    if not rows:
        logger.info("Curation: no listings.")
        return {}
    by_id = {r["id"]: r for r in rows}

    # Curation is authoritative for the derived columns — reset before recompute.
    db.reset_curation_columns()

    # 1. Operators
    operator_map = assign_operator_ids(rows)
    db.set_operator_ids(operator_map)
    logger.info("Curation: %d listings carry an operator_id", len(operator_map))

    # 2. Property groups
    group_map, cross_groups, identity_groups = assign_property_groups(rows, operator_map, dedup_cfg=getattr(config, "dedup", None))
    db.set_property_groups(group_map, cross_groups)
    logger.info("Curation: %d listings in %d property groups (%d cross-platform)",
                len(group_map), len(set(group_map.values())), len(cross_groups))

    # 3. Geocode Booking addresses + collect scraped/geocoded observations.
    geocfg = getattr(config, "geocoding", None)
    fusion_cfg = getattr(config, "fusion", None)
    geo_sigma = getattr(fusion_cfg, "sigma_geocoded_m", SIGMA_GEOCODED)
    geocoder = Geocoder(
        db, fetch_fn=fetch_fn,
        base_url=getattr(geocfg, "nominatim_url", "https://nominatim.openstreetmap.org/search"),
        user_agent=getattr(geocfg, "user_agent", "bucharest-str-research/1.0"),
        rate_limit_s=getattr(geocfg, "rate_limit_s", 1.0),
        timeout=getattr(geocfg, "timeout_seconds", 20),
        max_retries=getattr(geocfg, "max_retries", 5),
    ) if (geocfg is None or geocfg.enabled) else None

    stack = Counter((round(r["latitude"], 6), round(r["longitude"], 6)) for r in rows)

    observations: list[tuple] = []
    fuse_inputs: dict[str, list[Observation]] = defaultdict(list)
    geocoded_map: dict[str, tuple] = {}

    def group_key(lid: str) -> str:
        return group_map.get(lid, lid)  # singletons fuse on their own id

    for r in rows:
        lid = r["id"]
        gk = group_key(lid)
        _, sigma = classify_scraped_precision(
            r, stack[(round(r["latitude"], 6), round(r["longitude"], 6))], sigmas=fusion_cfg)
        cap_date = (r.get("scraped_at") or "")[:10]
        observations.append((lid, group_map.get(lid), cap_date, r["platform"],
                             "scraped", r["latitude"], r["longitude"], sigma))
        fuse_inputs[gk].append(Observation(lid, r["latitude"], r["longitude"], sigma, "scraped"))

        if geocoder and r["platform"] == "booking":
            address = extract_booking_address(r.get("raw_json"))
            if address:
                hit = geocoder.geocode(address)
                if hit:
                    geocoded_map[lid] = (hit[0], hit[1], address)
                    observations.append((lid, group_map.get(lid), cap_date, r["platform"],
                                        "geocoded", hit[0], hit[1], geo_sigma))
                    fuse_inputs[gk].append(Observation(lid, hit[0], hit[1], geo_sigma, "geocoded"))

    # 3b. Temporal backfill (historical observations from a prior capture).
    for (lid, lat, lng, sigma_m, cap_date, platform) in (backfill_rows or []):
        if lid in by_id:
            gk = group_key(lid)
            observations.append((lid, group_map.get(lid), cap_date, platform, "scraped", lat, lng, sigma_m))
            fuse_inputs[gk].append(Observation(lid, lat, lng, sigma_m, "scraped"))

    db.replace_position_observations(observations)
    db.set_geocoded(geocoded_map)

    # 4. Fuse each group; every member of a group gets the same best position.
    members_by_key: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        members_by_key[group_key(r["id"])].append(r["id"])

    fused_map: dict[str, dict] = {}
    exact_max = getattr(getattr(config, "fusion", None), "exact_max_sigma_m", 40.0)
    for gk, obs in fuse_inputs.items():
        fused = fuse_observations(obs)
        for lid in members_by_key.get(gk, []):
            if fused.dominant_listing_id != lid:
                source = "transferred_from_twin"
            elif fused.dominant_source == "geocoded":
                source = "geocoded_address"
            else:
                source = "platform_coord"
            fused_map[lid] = {
                "lat_best": fused.latitude, "lng_best": fused.longitude,
                "est_accuracy_m": round(fused.sigma_m, 1),
                "position_confidence": round(position_confidence(fused.sigma_m), 3),
                "location_source": source,
                "location_precision": "exact" if fused.sigma_m <= exact_max else "approximate",
            }
    db.set_fused_positions(fused_map)

    # Flag cross-platform groups whose Booking vs Airbnb observations disagree
    # by more than the configured distance — a probable false-positive link.
    disagreement_m = getattr(fusion_cfg, "disagreement_km", 1.0) * 1000.0
    geo_conflicts: list[str] = []
    for gk, obs in fuse_inputs.items():
        bk = [(o.latitude, o.longitude) for o in obs
              if o.listing_id in by_id and by_id[o.listing_id]["platform"] == "booking"]
        ab = [(o.latitude, o.longitude) for o in obs
              if o.listing_id in by_id and by_id[o.listing_id]["platform"] == "airbnb"]
        if bk and ab:
            maxd = max(haversine_distance(b[0], b[1], a[0], a[1]) for b in bk for a in ab)
            if maxd > disagreement_m:
                geo_conflicts.append(gk)
    if geo_conflicts:
        logger.warning("Curation: %d cross-platform groups disagree >%.0fm on position",
                       len(geo_conflicts), disagreement_m)

    # 5. Verification (exclude identity/operator-derived groups to avoid circularity)
    metrics = dedup_metrics(rows, group_map, identity_groups)
    metrics["geo_conflict_groups"] = geo_conflicts
    logger.info("Curation metrics: %s", metrics)
    try:
        from ..storage.exporter import export_dedup_metrics, export_dedup_review
        export_dedup_metrics(metrics)
        export_dedup_review(db)
    except Exception as e:
        logger.warning("Curation: review export failed (%s)", e)
    return metrics
