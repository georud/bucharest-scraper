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
              "est_accuracy_m", "position_confidence", "geocoded_address",
              "amenities", "cross_platform_offset_m", "cross_platform_offset_source"):
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


def test_missing_data_targets_partial_room(db):
    # beds set but bathrooms/bedrooms NULL -> still re-fetched (OR-any, not all-NULL).
    _mk(db, "airbnb_5", Platform.AIRBNB, "Partial", 44.43, 26.10, beds=2, max_guests=4)
    _mk(db, "airbnb_6", Platform.AIRBNB, "Complete", 44.44, 26.11,
        bedrooms=1, beds=2, bathrooms=1.0, max_guests=4)
    ids = {l.id for l in db.get_listings_missing_data(Platform.AIRBNB)}
    assert "airbnb_5" in ids
    assert "airbnb_6" not in ids


def test_missing_business_targets_host_stat_gap(db):
    # Airbnb with business_type set but a host stat NULL -> re-fetched via Phase 3.
    _mk(db, "airbnb_7", Platform.AIRBNB, "HostGap", 44.43, 26.10,
        business_type="Individual", host_response_rate="100%")  # host_join_date NULL
    _mk(db, "airbnb_8", Platform.AIRBNB, "HostFull", 44.44, 26.11,
        business_type="Individual", host_response_rate="100%", host_join_date="Joined in 2019")
    ids = {l.id for l in db.get_listings_missing_business_data(Platform.AIRBNB)}
    assert "airbnb_7" in ids
    assert "airbnb_8" not in ids


def test_clear_failed_geocodes_keeps_ok(db):
    db.upsert_geocode("addr_ok", "ok", 44.4, 26.1, "building", 1)
    db.upsert_geocode("addr_nf", "not_found", None, None, None, 1)
    db.upsert_geocode("addr_fail", "failed", None, None, None, 2)
    assert db.clear_failed_geocodes() == 2
    assert db.get_geocode("addr_ok") is not None
    assert db.get_geocode("addr_nf") is None
    assert db.get_geocode("addr_fail") is None


def test_airbnb_radius_capture_and_missing(db):
    _mk(db, "airbnb_r1", Platform.AIRBNB, "R1", 44.43, 26.10, url="https://www.airbnb.com/rooms/r1")
    _mk(db, "airbnb_r2", Platform.AIRBNB, "R2", 44.44, 26.11, url="https://www.airbnb.com/rooms/r2")
    # Both listings start with no radius and no amenities -> both need PDP details.
    assert {m["id"] for m in db.get_airbnb_listings_missing_pdp_details()} == {"airbnb_r1", "airbnb_r2"}
    assert all(m["url"] for m in db.get_airbnb_listings_missing_pdp_details())
    # Setting radius on r1 keeps it in the queue (amenities still NULL).
    db.set_airbnb_location_radius({"airbnb_r1": 0.0})
    assert {m["id"] for m in db.get_airbnb_listings_missing_pdp_details()} == {"airbnb_r1", "airbnb_r2"}
    assert len(db.get_airbnb_listings_missing_pdp_details(limit=1)) == 1


def test_platform_precision_and_reset_keeps_radius(db):
    _mk(db, "booking_pp", Platform.BOOKING, "B", 44.43, 26.10)
    db.set_platform_precision({"booking_pp": "exact"})
    db.set_airbnb_location_radius({"booking_pp": 0.0})
    assert db.conn.execute(
        "SELECT platform_precision FROM listings WHERE id='booking_pp'").fetchone()[0] == "exact"
    db.reset_curation_columns()
    row = db.conn.execute(
        "SELECT platform_precision, airbnb_location_radius_m FROM listings WHERE id='booking_pp'").fetchone()
    assert row[0] is None    # platform_precision is curation-derived -> reset
    assert row[1] == 0.0     # radius is scraper data -> preserved


def test_amenities_roundtrip_and_missing_pdp(db):
    _mk(db, "airbnb_a1", Platform.AIRBNB, "A", 44.43, 26.10, url="https://www.airbnb.com/rooms/a1")
    db.set_airbnb_location_radius({"airbnb_a1": 0.0})           # has radius, missing amenities
    assert "airbnb_a1" in {r["id"] for r in db.get_airbnb_listings_missing_pdp_details()}
    db.set_airbnb_amenities({"airbnb_a1": '["wifi","kitchen"]'})
    assert db.conn.execute("SELECT amenities FROM listings WHERE id='airbnb_a1'").fetchone()[0] == '["wifi","kitchen"]'
    assert "airbnb_a1" not in {r["id"] for r in db.get_airbnb_listings_missing_pdp_details()}  # now complete


def test_curation_cols_include_maxguests_amenities(db):
    _mk(db, "airbnb_a2", Platform.AIRBNB, "A", 44.43, 26.10, max_guests=8)
    db.set_airbnb_amenities({"airbnb_a2": '["tv"]'})
    row = next(r for r in db.get_listings_for_curation() if r["id"] == "airbnb_a2")
    assert row["max_guests"] == 8 and row["amenities"] == '["tv"]'


def test_cross_platform_offset_roundtrip_and_reset(db):
    _mk(db, "airbnb_off1", Platform.AIRBNB, "A", 44.43, 26.10)
    db.set_cross_platform_offsets({"airbnb_off1": (123.4, "geocoded")})
    row = db.conn.execute(
        "SELECT cross_platform_offset_m, cross_platform_offset_source FROM listings WHERE id='airbnb_off1'"
    ).fetchone()
    assert row[0] == 123.4 and row[1] == "geocoded"
    db.reset_curation_columns()
    row = db.conn.execute(
        "SELECT cross_platform_offset_m, cross_platform_offset_source FROM listings WHERE id='airbnb_off1'"
    ).fetchone()
    assert row[0] is None and row[1] is None       # curation-derived -> cleared
