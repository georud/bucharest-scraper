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


from src.dedup.operators import assign_operator_ids


class _Row(dict):
    """dict with attribute access, mimicking a curation row."""
    __getattr__ = dict.get


def _row(id, reg=None, phone=None, email=None):
    return _Row(id=id, business_registration_number=reg,
                business_phone=phone, business_email=email)


def test_operator_union_find_links_by_shared_phone():
    rows = [
        _row("booking_1", phone="+40 721 000 111"),
        _row("airbnb_2", phone="0721000111"),     # same phone -> same operator
        _row("airbnb_3", reg="RO 999"),           # different operator
    ]
    mapping = assign_operator_ids(rows)
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping["airbnb_3"] != mapping["booking_1"]


def test_operator_id_is_stable_and_listings_without_keys_excluded():
    rows = [_row("booking_1", reg="RO123"), _row("airbnb_9")]
    mapping = assign_operator_ids(rows)
    assert "airbnb_9" not in mapping            # no identity key -> no operator
    assert mapping["booking_1"].startswith("op_")
