from __future__ import annotations

import json
import re

# Default per-source sigmas (metres). Overridable via FusionConfig.
SIGMA_GEOCODED = 25.0
SIGMA_BOOKING_ADDRESS = 50.0
SIGMA_VAGUE = 150.0
SIGMA_AIRBNB = 100.0

_STREET_RE = re.compile(
    r"^\d+\s|strada|str\.|calea|bulevardul|bd\.|soseaua|sos\.|aleea|bloc|apartament|ap\.",
    re.IGNORECASE,
)


def extract_booking_address(raw_json: str | None) -> str | None:
    """Pull 'address, city' from a Booking raw_json location block. The real
    payload nests it under basicPropertyData.location; older/synthetic payloads
    may put it at the top level, so try both."""
    if not raw_json:
        return None
    try:
        obj = json.loads(raw_json) or {}
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    loc = (obj.get("basicPropertyData") or {}).get("location") or obj.get("location") or {}
    if not isinstance(loc, dict):
        return None
    address = (loc.get("address") or "").strip()
    if not address:
        return None
    city = (loc.get("city") or "").strip()
    return f"{address}, {city}" if city else address


def _is_street_level(address: str | None) -> bool:
    return bool(address and _STREET_RE.search(address))


def classify_scraped_precision(row: dict, stack_count: int, sigmas=None) -> tuple[str, float]:
    """Return (provisional_precision, sigma_m) for a listing's scraped coordinate.

    The sigma seeds the observation ledger; the listing's authoritative
    location_precision is decided later by fusion. `stack_count` is how many
    listings share this exact coordinate (>=3 => centroid => approximate)."""
    s_booking = getattr(sigmas, "sigma_booking_address_m", SIGMA_BOOKING_ADDRESS)
    s_vague = getattr(sigmas, "sigma_vague_m", SIGMA_VAGUE)
    s_airbnb = getattr(sigmas, "sigma_airbnb_m", SIGMA_AIRBNB)
    if row["platform"] == "booking":
        address = extract_booking_address(row.get("raw_json"))
        if _is_street_level(address) and stack_count < 3:
            return "approximate", s_booking
        return "approximate", s_vague
    return "approximate", s_airbnb
