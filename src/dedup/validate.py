from __future__ import annotations

from collections import defaultdict

from .operators import normalize_registration, normalize_phone, normalize_email


def _identity_keys(row: dict) -> set[str]:
    keys = set()
    for fn, field in (
        (normalize_registration, "business_registration_number"),
        (normalize_phone, "business_phone"),
        (normalize_email, "business_email"),
    ):
        k = fn(row.get(field))
        if k:
            keys.add(field[:3] + ":" + k)
    return keys


def dedup_metrics(rows, mapping: dict[str, str], excluded_groups: set[str]) -> dict:
    """Identity-key ground-truth check on proximity/name (Tier 2) property groups.

    A group is 'comparable' if >=2 of its members carry any identity key. It
    'agrees' if all such members share at least one key; otherwise it is a
    conflict (false-positive suspect). Identity/operator-derived groups (Tier
    0/1, passed as `excluded_groups`) are excluded to avoid circularity. Returns
    precision/recall proxies + conflicts.
    """
    by_id = {r["id"]: r for r in rows}
    members: dict[str, list[str]] = defaultdict(list)
    for lid, gid in mapping.items():
        members[gid].append(lid)

    comparable = agreeing = 0
    conflicts: list[str] = []
    for gid, ids in members.items():
        if gid in excluded_groups:
            continue
        keyed = [by_id[i] for i in ids if i in by_id and _identity_keys(by_id[i])]
        if len(keyed) < 2:
            continue
        comparable += 1
        common = set.intersection(*[_identity_keys(r) for r in keyed])
        if common:
            agreeing += 1
        else:
            conflicts.append(gid)

    # Recall proxy: identity-confirmed cross-platform twins we DID group.
    key_pairs = defaultdict(lambda: {"booking": set(), "airbnb": set()})
    for r in rows:
        for k in _identity_keys(r):
            key_pairs[k][r["platform"]].add(r["id"])
    singleton_twins = [
        (next(iter(v["booking"])), next(iter(v["airbnb"])))
        for v in key_pairs.values()
        if len(v["booking"]) == 1 and len(v["airbnb"]) == 1
    ]
    grouped = sum(1 for b, a in singleton_twins
                  if mapping.get(b) and mapping.get(b) == mapping.get(a))

    return {
        "comparable_groups": comparable,
        "agreeing_groups": agreeing,
        "precision_proxy": round(agreeing / comparable, 4) if comparable else None,
        "conflict_groups": conflicts,
        "identity_twins": len(singleton_twins),
        "identity_twins_grouped": grouped,
        "recall_proxy": round(grouped / len(singleton_twins), 4) if singleton_twins else None,
    }
