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
