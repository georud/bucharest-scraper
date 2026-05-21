from src.dedup.validate import dedup_metrics


def _row(id, platform, reg=None, phone=None):
    return {"id": id, "platform": platform,
            "business_registration_number": reg, "business_phone": phone}


def test_metrics_flag_identity_conflict():
    rows = [
        _row("booking_1", "booking", reg="RO111"),
        _row("airbnb_2", "airbnb", reg="RO111"),   # agrees -> good
        _row("booking_3", "booking", reg="RO222"),
        _row("airbnb_4", "airbnb", reg="RO999"),    # conflict within a group
    ]
    # Tier 1/2 groups (proximity/name matched): {1,2} agree, {3,4} conflict.
    mapping = {"booking_1": "pg_a", "airbnb_2": "pg_a",
               "booking_3": "pg_b", "airbnb_4": "pg_b"}
    excluded_groups = set()  # both groups matched by proximity/name (Tier 2)
    m = dedup_metrics(rows, mapping, excluded_groups)
    assert m["comparable_groups"] == 2
    assert m["agreeing_groups"] == 1
    assert m["precision_proxy"] == 0.5
    assert "pg_b" in m["conflict_groups"]
