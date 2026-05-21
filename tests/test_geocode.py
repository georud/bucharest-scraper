from src.geo.geocode import normalize_address, Geocoder


def test_normalize_address():
    assert normalize_address("  36 Strada X,  Bucuresti ") == "36 strada x, bucuresti"


def test_geocoder_caches_success(db):
    calls = {"n": 0}

    def fake_fetch(query):
        calls["n"] += 1
        return [{"lat": "44.43", "lon": "26.10", "category": "building"}]

    g = Geocoder(db, fetch_fn=fake_fetch, rate_limit_s=0, max_retries=5)
    r1 = g.geocode("36 Strada X, Bucuresti")
    r2 = g.geocode("36 Strada X, Bucuresti")  # served from cache
    assert r1 == (44.43, 26.10)
    assert r2 == (44.43, 26.10)
    assert calls["n"] == 1  # second call hit the cache


def test_geocoder_retries_failures_until_cap(db):
    def failing_fetch(query):
        raise TimeoutError("boom")

    g = Geocoder(db, fetch_fn=failing_fetch, rate_limit_s=0, max_retries=3)
    assert g.geocode("X, Bucuresti") is None
    cached = db.get_geocode(normalize_address("X, Bucuresti"))
    assert cached["status"] == "failed"
    assert cached["attempts"] == 1  # one attempt this run; re-tried on later runs

    # A later run retries (attempts increments) until it reaches the cap.
    g.geocode("X, Bucuresti")
    assert db.get_geocode(normalize_address("X, Bucuresti"))["attempts"] == 2
