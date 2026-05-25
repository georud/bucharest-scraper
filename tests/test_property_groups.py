from src.dedup.property_groups import room_config_matches, assign_property_groups


def _row(id, platform, name, lat, lng, reg=None, phone=None, email=None,
         bedrooms=None, beds=None, bathrooms=None):
    return {"id": id, "platform": platform, "name": name,
            "latitude": lat, "longitude": lng,
            "business_registration_number": reg, "business_phone": phone,
            "business_email": email, "bedrooms": bedrooms, "beds": beds,
            "bathrooms": bathrooms}


def test_room_config_matches():
    assert room_config_matches({"bedrooms": 2, "beds": 3, "bathrooms": 1},
                               {"bedrooms": 2, "beds": None, "bathrooms": 1})
    assert not room_config_matches({"bedrooms": 2}, {"bedrooms": 3})
    assert not room_config_matches({"bedrooms": None}, {"bedrooms": None})  # nothing comparable


def test_tier0_singleton_identity_links_despite_distance():
    # Same unique phone, but Airbnb point fuzzed ~400 m away and a different name.
    rows = [
        _row("booking_1", "booking", "Central Studio", 44.4300, 26.1000, phone="0721000111"),
        _row("airbnb_2", "airbnb", "Cozy Place Downtown", 44.4336, 26.1000, phone="+40721000111"),
    ]
    mapping, cross, _ident = assign_property_groups(rows, operator_map={"booking_1": "op_a", "airbnb_2": "op_a"})
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping["booking_1"] in cross  # spans both platforms


def test_shared_operator_many_units_does_not_overmerge():
    # One operator (shared phone) with two genuinely different flats far apart.
    rows = [
        _row("booking_1", "booking", "Flat A Unirii", 44.4270, 26.1020, phone="0720000001"),
        _row("airbnb_2", "airbnb", "Flat A Unirii", 44.4270, 26.1020, phone="0720000001"),
        _row("booking_3", "booking", "Flat B Aviatorilor", 44.4700, 26.0870, phone="0720000001"),
    ]
    op = {"booking_1": "op_a", "airbnb_2": "op_a", "booking_3": "op_a"}
    mapping, cross, _ident = assign_property_groups(rows, operator_map=op)
    # The two co-located A flats group; the distant B flat must NOT join them.
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping.get("booking_3") != mapping["booking_1"]


# ---------------------------------------------------------------------------
# Same-platform distinctness veto tests
# ---------------------------------------------------------------------------
from src.dedup.property_groups import _compatible


def _r(pid, plat="airbnb", lat=44.4300, lng=26.1000, name="Flat", mg=None, am=None,
       bed=4, beds=6, bath=2.5):
    return {"id": pid, "platform": plat, "latitude": lat, "longitude": lng, "name": name,
            "bedrooms": bed, "beds": beds, "bathrooms": bath, "max_guests": mg, "amenities": am}


def test_veto_max_guests_blocks_both_tiers():
    a = _r("a", mg=12); b = _r("b", lat=44.4309, name="Flat", mg=14)   # ~100 m, same room, diff capacity
    assert _compatible(a, b, relaxed=True) is False     # Tier-1 (operator block) vetoed
    a2 = _r("a", name="Cozy Central Studio", mg=12)
    b2 = _r("b", lat=44.4305, name="Cozy Central Studio", mg=14)        # name ~100, <100 m -> Tier-2 candidate
    assert _compatible(a2, b2, relaxed=False) is False  # Tier-2 vetoed too


def test_veto_amenities_low_jaccard():
    a = _r("a", mg=8, am='["wifi","kitchen","tv","ac"]')
    b = _r("b", lat=44.4305, mg=8, am='["pool","gym","sauna","parking"]')  # same capacity, disjoint amenities
    assert _compatible(a, b, relaxed=True) is False


def test_veto_amenities_at_threshold_not_vetoed():
    # Jaccard exactly 0.6 (3 shared / 5 union) is NOT < 0.6 -> no veto (pins the strict-< contract).
    a = _r("a", mg=8, am='["wifi","kitchen","tv","ac"]')
    b = _r("b", lat=44.4305, mg=8, am='["wifi","kitchen","tv","pool"]')
    assert _compatible(a, b, relaxed=True) is True


def test_veto_noop_cross_platform_and_missing():
    # Different platform -> never vetoed (Booking has no max_guests/amenities)
    a = _r("a", plat="booking", mg=None); b = _r("b", plat="airbnb", lat=44.4305, mg=14)
    assert _compatible(a, b, relaxed=True) is True      # falls through to room/dist match -> compatible
    # Same platform but capacity unknown + amenities absent -> no veto, still compatible
    c = _r("c", mg=None, am=None); d = _r("d", lat=44.4305, mg=None, am=None)
    assert _compatible(c, d, relaxed=True) is True
