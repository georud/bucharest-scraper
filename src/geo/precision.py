from __future__ import annotations

import json
import re

# Default per-source sigmas (metres). Overridable via FusionConfig.
SIGMA_GEOCODED = 25.0
SIGMA_BOOKING_ADDRESS = 50.0
SIGMA_VAGUE = 150.0
SIGMA_AIRBNB = 100.0
SIGMA_AIRBNB_EXACT = 15.0  # Airbnb mapMarkerRadiusInMeters==0 => exact coord (rounding limit)

_STREET_RE = re.compile(
    r"^\d+\s|strada|str\.|calea|bulevardul|bd\.|soseaua|sos\.|aleea|bloc|apartament|ap\.",
    re.IGNORECASE,
)

# Apartment-level / building tokens — truncate the address at the first one.
# `sc\d+`/`bl\d+` catch attached codes like "sc1"; bare `sc`/`bl`/`et`/`ap`
# catch the spelled abbreviations.
_GEOCODE_NOISE = re.compile(
    r"\b(?:bloc|bl|sc\d+|bl\d+|scara|sc|etaj|et|apartament|apart|ap|corp|floor|"
    r"apartment|room|parter|casa|cladirea|cladire|building|demisol|subsol|mansarda)\b",
    re.IGNORECASE,
)
# "nr."/"nr" is a number prefix — drop the word but keep the number that follows.
_NR_RE = re.compile(r"\s*,?\s*\bnr\b\.?\s*", re.IGNORECASE)
# A number range ("94-100") — keep the first value.
_RANGE_RE = re.compile(r"(\d+)\s*[-–]\s*\d+")
# A trailing block code after a comma ("..., A2", "..., 12B") — drop it. Requires
# a letter so a bare trailing street number ("..., 12") is preserved.
_TRAILING_CODE_RE = re.compile(r",\s*(?:[a-z]{1,3}\s*\d+|\d+\s*[a-z])\s*$", re.IGNORECASE)


def _clean_street(address: str) -> str:
    a = _RANGE_RE.sub(r"\1", address)
    a = _NR_RE.sub(" ", a)
    a = _GEOCODE_NOISE.split(a, maxsplit=1)[0]
    a = _TRAILING_CODE_RE.sub("", a)
    return a.strip(" ,.")


def extract_booking_address(raw_json: str | None) -> str | None:
    """Return a geocodable 'street+number, city' from a Booking raw_json
    location block (basicPropertyData.location, fallback top-level location).
    Apartment/floor/building detail and `nr.` prefixes are stripped and number
    ranges collapsed so Nominatim can resolve the street; None if no address."""
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
    address = _clean_street((loc.get("address") or "").strip())
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
    # Airbnb: mapMarkerRadiusInMeters==0 means the host exposes the exact location,
    # so the scraped coordinate is precise; >0 is the obfuscation radius; NULL =
    # not captured => fall back to the fuzzed default.
    radius = row.get("airbnb_location_radius_m")
    if radius is not None:
        if radius == 0:
            return "exact", SIGMA_AIRBNB_EXACT
        return "approximate", max(s_airbnb, float(radius) * 0.7)
    return "approximate", s_airbnb
