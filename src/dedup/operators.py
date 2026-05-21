from __future__ import annotations

import hashlib
import re


def normalize_registration(value: str | None) -> str | None:
    """Lowercase, strip to [a-z0-9], drop a leading 'ro' VAT prefix."""
    if not value:
        return None
    cleaned = re.sub(r"[^a-z0-9]", "", value.lower())
    if cleaned.startswith("ro"):
        cleaned = cleaned[2:]
    return cleaned or None


def normalize_phone(value: str | None) -> str | None:
    """Digits only; drop RO trunk/country prefixes; keep the 9-digit national
    number. Returns None if fewer than 9 digits (too weak to be an identity key)."""
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    for prefix in ("0040", "40", "0"):
        if digits.startswith(prefix) and len(digits) - len(prefix) >= 9:
            digits = digits[len(prefix):]
            break
    if len(digits) < 9:
        return None
    return digits[-9:]


def normalize_email(value: str | None) -> str | None:
    """Lowercase + trim; return None if it isn't a plausible email."""
    if not value:
        return None
    cleaned = value.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", cleaned):
        return None
    return cleaned


def assign_operator_ids(rows) -> dict[str, str]:
    """Union-find over listings sharing any normalized identity key
    (registration / phone / email). Returns {listing_id: operator_id} for
    listings that carry at least one identity key. operator_id is a stable hash
    of the operator's sorted identity keys.

    Safe to use union-find here: a shared registration/phone/email genuinely
    identifies one operator (unlike GPS+name, which can chain distinct flats).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # key -> first listing id seen with that key; union subsequent sharers.
    key_owner: dict[str, str] = {}
    has_key: set[str] = set()
    for row in rows:
        keys = [
            normalize_registration(row.get("business_registration_number")),
            normalize_phone(row.get("business_phone")),
            normalize_email(row.get("business_email")),
        ]
        for key in keys:
            if not key:
                continue
            has_key.add(row["id"])
            find(row["id"])
            if key in key_owner:
                union(key_owner[key], row["id"])
            else:
                key_owner[key] = row["id"]

    # Collect the identity keys per component to build a stable operator_id.
    comp_keys: dict[str, set[str]] = {}
    for row in rows:
        if row["id"] not in has_key:
            continue
        root = find(row["id"])
        bucket = comp_keys.setdefault(root, set())
        for key in (
            normalize_registration(row.get("business_registration_number")),
            normalize_phone(row.get("business_phone")),
            normalize_email(row.get("business_email")),
        ):
            if key:
                bucket.add(key)

    root_to_opid = {
        root: "op_" + hashlib.md5("|".join(sorted(keys)).encode()).hexdigest()[:16]
        for root, keys in comp_keys.items()
    }
    return {lid: root_to_opid[find(lid)] for lid in has_key}
