from src.dedup.operators import (
    normalize_registration, normalize_phone, normalize_email,
)


def test_normalize_registration_strips_ro_and_punctuation():
    assert normalize_registration("RO 41137103") == "41137103"
    assert normalize_registration("J40/1234/2020") == "j4012342020"
    assert normalize_registration(None) is None
    assert normalize_registration("   ") is None


def test_normalize_phone_keeps_last_9_digits():
    assert normalize_phone("+40 721 234 567") == "721234567"
    assert normalize_phone("0040721234567") == "721234567"
    assert normalize_phone("0721234567") == "721234567"
    assert normalize_phone("123") is None  # too short to be an identity key


def test_normalize_email_lowercases_and_trims():
    assert normalize_email("  Host@Example.COM ") == "host@example.com"
    assert normalize_email("not-an-email") is None
