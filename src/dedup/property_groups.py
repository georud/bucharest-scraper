from __future__ import annotations

import hashlib
from collections import defaultdict

from rapidfuzz import fuzz

from .deduplicator import haversine_distance
from .operators import normalize_registration, normalize_phone, normalize_email

# Tier thresholds (overridable via config in the curation stage).
TIER1_RELAXED_DISTANCE_M = 250.0
TIER1_NAME_THRESHOLD = 70.0
TIER2_STRICT_DISTANCE_M = 100.0
TIER2_NAME_THRESHOLD = 80.0


def room_config_matches(a: dict, b: dict) -> bool:
    """True if every room field present on BOTH sides is equal, and at least one
    field is comparable. None is treated as wildcard."""
    comparable = 0
    for field in ("bedrooms", "beds", "bathrooms"):
        av, bv = a.get(field), b.get(field)
        if av is None or bv is None:
            continue
        comparable += 1
        if av != bv:
            return False
    return comparable > 0


def _name_sim(a: dict, b: dict) -> float:
    return fuzz.ratio((a.get("name") or "").lower(), (b.get("name") or "").lower())


def _identity_keys(row: dict) -> set[str]:
    keys = set()
    for fn, field in (
        (normalize_registration, "business_registration_number"),
        (normalize_phone, "business_phone"),
        (normalize_email, "business_email"),
    ):
        k = fn(row.get(field))
        if k:
            keys.add(field[:3] + ":" + k)  # namespaced so phone != reg collision-free
    return keys


def _compatible(a: dict, b: dict, relaxed: bool,
                relaxed_dist: float = TIER1_RELAXED_DISTANCE_M,
                strict_dist: float = TIER2_STRICT_DISTANCE_M,
                strict_name: float = TIER2_NAME_THRESHOLD) -> bool:
    """Geographic + name (+ room-config) compatibility, used to guard group
    growth. Identity keys are deliberately NOT consulted here (operator-level);
    single-property identity linking is the Tier-0 candidate path."""
    dist = haversine_distance(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
    if relaxed:
        return dist <= relaxed_dist and (
            _name_sim(a, b) >= TIER1_NAME_THRESHOLD or room_config_matches(a, b))
    return dist <= strict_dist and _name_sim(a, b) >= strict_name


def assign_property_groups(rows, operator_map: dict[str, str], dedup_cfg=None):
    """Group listings that are the same physical flat (within or across
    platforms). Returns (mapping, cross_platform_groups, identity_groups):
      mapping: {listing_id: group_id} for listings in a multi-member group
      cross_platform_groups: set of group_ids whose members span both platforms
      identity_groups: group_ids formed using identity/operator signals (Tier
        0/1) — excluded from the verification precision proxy to avoid circularity

    Three candidate tiers, highest confidence first:
      Tier 0: an identity key (phone/email/reg) mapping to exactly one Booking
              and exactly one Airbnb listing -> direct link (single-property host).
      Tier 1: within a shared operator block, relaxed distance + name/room match.
      Tier 2: outside operator blocks, tightened distance + name.
    Greedy with a clique-compatibility check so groups can't chain distant flats.
    """
    by_id = {r["id"]: r for r in rows}

    relaxed_dist = getattr(dedup_cfg, "operator_relaxed_distance_m", TIER1_RELAXED_DISTANCE_M)
    strict_dist = getattr(dedup_cfg, "strict_distance_m", TIER2_STRICT_DISTANCE_M)
    strict_name = getattr(dedup_cfg, "strict_name_threshold", TIER2_NAME_THRESHOLD)

    def compat(x, y, relaxed):
        return _compatible(x, y, relaxed, relaxed_dist, strict_dist, strict_name)

    # ---- Tier 0: singleton identity across platforms ----
    key_to_booking: dict[str, list[str]] = defaultdict(list)
    key_to_airbnb: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        for k in _identity_keys(r):
            (key_to_booking if r["platform"] == "booking" else key_to_airbnb)[k].append(r["id"])

    candidates: list[tuple[int, float, str, str]] = []  # (tier, -confidence, id_a, id_b)
    for k in set(key_to_booking) & set(key_to_airbnb):
        if len(key_to_booking[k]) == 1 and len(key_to_airbnb[k]) == 1:
            candidates.append((0, -1.0, key_to_booking[k][0], key_to_airbnb[k][0]))

    # ---- Tier 1: within operator blocks ----
    blocks: dict[str, list[str]] = defaultdict(list)
    for lid, op in operator_map.items():
        if lid in by_id:
            blocks[op].append(lid)
    for members in blocks.values():
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a, b = by_id[members[i]], by_id[members[j]]
                if compat(a, b, True):
                    candidates.append((1, -_name_sim(a, b) / 100.0, a["id"], b["id"]))

    # ---- Tier 2: spatial bucket (tightened) ----
    buckets: dict[tuple[float, float], list[str]] = defaultdict(list)
    for r in rows:
        buckets[(round(r["latitude"], 3), round(r["longitude"], 3))].append(r["id"])
    for r in rows:
        blat, blng = round(r["latitude"], 3), round(r["longitude"], 3)
        for dlat in (-0.001, 0.0, 0.001):
            for dlng in (-0.001, 0.0, 0.001):
                for other_id in buckets.get((round(blat + dlat, 3), round(blng + dlng, 3)), ()):
                    if other_id <= r["id"]:
                        continue
                    a, b = r, by_id[other_id]
                    if compat(a, b, False):
                        candidates.append((2, -_name_sim(a, b) / 100.0, a["id"], b["id"]))

    candidates.sort()  # tier asc, then -confidence asc (best first)

    # ---- Greedy union with clique check ----
    groups: dict[str, set[str]] = {}     # temp group_id -> members
    of: dict[str, str] = {}              # listing_id -> temp group_id
    identity_tmp: set[str] = set()       # temp gids formed using identity/operator (tier 0/1)
    counter = 0

    for tier, _conf, a_id, b_id in candidates:
        relaxed = tier != 2
        ga, gb = of.get(a_id), of.get(b_id)
        if ga and ga == gb:
            continue
        target: str | None = None
        if ga is None and gb is None:
            counter += 1
            target = f"_tmp_{counter}"
            groups[target] = {a_id, b_id}
            of[a_id] = of[b_id] = target
        elif ga and gb is None:
            if all(compat(by_id[b_id], by_id[m], relaxed) for m in groups[ga]):
                groups[ga].add(b_id)
                of[b_id] = ga
                target = ga
        elif gb and ga is None:
            if all(compat(by_id[a_id], by_id[m], relaxed) for m in groups[gb]):
                groups[gb].add(a_id)
                of[a_id] = gb
                target = gb
        else:  # both in different groups
            if all(compat(by_id[x], by_id[y], relaxed)
                   for x in groups[ga] for y in groups[gb]):
                gb_ident = gb in identity_tmp
                groups[ga] |= groups[gb]
                for m in groups[gb]:
                    of[m] = ga
                del groups[gb]
                identity_tmp.discard(gb)
                if gb_ident:
                    identity_tmp.add(ga)
                target = ga
        if target is not None and tier in (0, 1):
            identity_tmp.add(target)

    # ---- Stable ids + cross-platform / identity flags ----
    mapping: dict[str, str] = {}
    cross: set[str] = set()
    identity_groups: set[str] = set()
    for tmp_gid, members in groups.items():
        gid = "pg_" + hashlib.md5("|".join(sorted(members)).encode()).hexdigest()[:16]
        platforms = {by_id[m]["platform"] for m in members}
        for m in members:
            mapping[m] = gid
        if {"booking", "airbnb"} <= platforms:
            cross.add(gid)
        if tmp_gid in identity_tmp:
            identity_groups.add(gid)
    return mapping, cross, identity_groups
