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
