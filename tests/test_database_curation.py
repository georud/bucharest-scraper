from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing


def _mk(db, lid, platform, name, lat, lng, **kw):
    db.upsert_listing(Listing(
        id=lid, platform=platform, platform_id=lid.split("_")[1],
        name=name, latitude=lat, longitude=lng,
        scraped_at=datetime(2026, 5, 15), first_seen_at=datetime(2026, 5, 15), **kw,
    ))


def test_new_columns_present(db):
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(listings)")}
    for c in ("operator_id", "property_group_id", "latitude_geocoded",
              "latitude_best", "location_precision", "location_source",
              "est_accuracy_m", "position_confidence", "geocoded_address"):
        assert c in cols


def test_new_tables_present(db):
    names = {r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "position_observations" in names
    assert "geocode_cache" in names


def test_get_listings_for_curation_and_setters(db):
    _mk(db, "booking_1", Platform.BOOKING, "Old Town Flat", 44.43, 26.10,
        business_phone="0721000111", raw_json='{"location":{"address":"Strada X 5","city":"Bucuresti"}}')
    _mk(db, "airbnb_2", Platform.AIRBNB, "Old Town Flat", 44.43, 26.10,
        business_phone="+40721000111")

    rows = db.get_listings_for_curation()
    assert {r["id"] for r in rows} == {"booking_1", "airbnb_2"}
    assert any(r["business_phone"] for r in rows)

    db.set_operator_ids({"booking_1": "op_x", "airbnb_2": "op_x"})
    db.set_property_groups({"booking_1": "pg_1", "airbnb_2": "pg_1"}, cross_platform={"pg_1"})
    got = {r[0]: tuple(r[1:]) for r in db.conn.execute(
        "SELECT id, operator_id, property_group_id, cross_platform_group_id FROM listings")}
    assert got["booking_1"] == ("op_x", "pg_1", "pg_1")
    assert got["airbnb_2"] == ("op_x", "pg_1", "pg_1")


def test_set_property_groups_non_cross_sets_null_cross_id(db):
    _mk(db, "booking_1", Platform.BOOKING, "Flat A", 44.43, 26.10)
    _mk(db, "booking_3", Platform.BOOKING, "Flat B", 44.43, 26.10)
    db.set_property_groups({"booking_1": "pg_x", "booking_3": "pg_x"}, cross_platform=set())
    got = {r[0]: (r[1], r[2]) for r in db.conn.execute(
        "SELECT id, property_group_id, cross_platform_group_id FROM listings")}
    assert got["booking_1"] == ("pg_x", None)
    assert got["booking_3"] == ("pg_x", None)


def test_geocode_cache_roundtrip(db):
    db.upsert_geocode("addr", "ok", 44.4, 26.1, "building", 1)
    result = db.get_geocode("addr")
    assert result is not None
    assert result["status"] == "ok"
    assert result["latitude"] == 44.4
    assert db.get_geocode("missing") is None
    db.upsert_geocode("addr", "ok", 44.4, 26.1, "building", 2)
    updated = db.get_geocode("addr")
    assert updated["attempts"] == 2


def test_replace_position_observations_is_idempotent(db):
    _mk(db, "booking_1", Platform.BOOKING, "Flat A", 44.43, 26.10)
    _mk(db, "airbnb_2", Platform.AIRBNB, "Flat B", 44.43, 26.10)
    db.replace_position_observations([
        ("booking_1", None, "2026-05-15", "booking", "scraped", 44.4, 26.1, 50.0)
    ])
    count = db.conn.execute("SELECT COUNT(*) FROM position_observations").fetchone()[0]
    assert count == 1
    db.replace_position_observations([
        ("airbnb_2", None, "2026-05-15", "airbnb", "scraped", 44.43, 26.10, 50.0)
    ])
    count = db.conn.execute("SELECT COUNT(*) FROM position_observations").fetchone()[0]
    assert count == 1


def test_set_fused_positions_writes_columns(db):
    _mk(db, "booking_1", Platform.BOOKING, "Flat A", 44.43, 26.10)
    db.set_fused_positions({"booking_1": {
        "lat_best": 44.43,
        "lng_best": 26.10,
        "est_accuracy_m": 25.0,
        "position_confidence": 0.83,
        "location_source": "geocoded_address",
        "location_precision": "exact",
    }})
    row = db.conn.execute(
        "SELECT latitude_best, location_precision FROM listings WHERE id='booking_1'"
    ).fetchone()
    assert row[0] == 44.43
    assert row[1] == "exact"
