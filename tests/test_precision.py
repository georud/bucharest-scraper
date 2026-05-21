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
