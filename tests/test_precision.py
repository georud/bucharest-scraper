from src.geo.precision import extract_booking_address, classify_scraped_precision

BK = '{"basicPropertyData": {"location": {"address": "36 Strada Moise Nicoara bloc D2, scara B, etaj 2, apartament 56", "city": "Bucuresti"}}}'
BK_VAGUE = '{"basicPropertyData": {"location": {"address": "Sector 3", "city": "Bucuresti"}}}'
BK_TOPLEVEL = '{"location": {"address": "10 Calea Victoriei", "city": "Bucuresti"}}'  # fallback path


def test_extract_booking_address():
    assert extract_booking_address(BK) == "36 Strada Moise Nicoara, Bucuresti"   # apartment-level noise stripped
    assert extract_booking_address(BK_TOPLEVEL) == "10 Calea Victoriei, Bucuresti"  # fallback path still works
    assert extract_booking_address('{"x":1}') is None
    # 'Sector 3' has no street/number and no noise token -> returned as-is (classified vague downstream)
    assert extract_booking_address(BK_VAGUE) == "Sector 3, Bucuresti"


def test_classify_booking_street_level():
    prec, sigma = classify_scraped_precision({"platform": "booking", "raw_json": BK}, stack_count=1)
    assert sigma == 50.0


def test_classify_booking_vague_or_stacked():
    _, sigma = classify_scraped_precision({"platform": "booking", "raw_json": BK_VAGUE}, stack_count=1)
    assert sigma == 150.0
    _, sigma_stacked = classify_scraped_precision({"platform": "booking", "raw_json": BK}, stack_count=5)
    assert sigma_stacked == 150.0


def test_classify_airbnb_always_fuzzed():
    _, sigma = classify_scraped_precision({"platform": "airbnb", "raw_json": None}, stack_count=1)
    assert sigma == 100.0


def _bk(address, city="Bucuresti"):
    import json
    return json.dumps({"basicPropertyData": {"location": {"address": address, "city": city}}})


def test_clean_address_handles_residual_noise():
    # Real geocode-failure patterns: nr. prefix, casa/parter/building tokens,
    # trailing block code, number range — all reduced to 'street number, city'.
    assert extract_booking_address(_bk("Strada Guraslau nr.27 casa 4", "Magurele")) == "Strada Guraslau 27, Magurele"
    assert extract_booking_address(_bk("Bulevardul Iuliu Maniu, nr.484")) == "Bulevardul Iuliu Maniu 484, Bucuresti"
    assert extract_booking_address(_bk("Drumul Bacriului 12 parter")) == "Drumul Bacriului 12, Bucuresti"
    assert extract_booking_address(_bk("24 Strada Preciziei, A2 building", "Bucharest")) == "24 Strada Preciziei, Bucharest"
    assert extract_booking_address(_bk("Prelungirea Ghencea 94-100")) == "Prelungirea Ghencea 94, Bucuresti"
    # A bare trailing street number must NOT be stripped as a block code.
    assert extract_booking_address(_bk("Strada Lipscani, 5")) == "Strada Lipscani, 5, Bucuresti"
