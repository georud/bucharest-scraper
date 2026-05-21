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
