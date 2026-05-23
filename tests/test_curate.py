from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing
from src.geo.curate import run_curation


def _mk(db, lid, platform, name, lat, lng, **kw):
    db.upsert_listing(Listing(
        id=lid, platform=platform, platform_id=lid.split("_")[1], name=name,
        latitude=lat, longitude=lng, scraped_at=datetime(2026, 5, 15),
        first_seen_at=datetime(2026, 5, 15), **kw))


def test_run_curation_links_twin_and_improves_airbnb_position(db):
    # Booking (precise street address) + Airbnb (fuzzed ~330 m, same phone).
    _mk(db, "booking_1", Platform.BOOKING, "Central Studio", 44.4300, 26.1000,
        business_phone="0721000111",
        raw_json='{"location":{"address":"5 Strada Lipscani","city":"Bucuresti"}}')
    _mk(db, "airbnb_2", Platform.AIRBNB, "Cozy Downtown", 44.4330, 26.1000,
        business_phone="+40721000111")

    # Inject a geocoder that returns a precise point for the Booking address.
    def fake_fetch(query):
        return [{"lat": "44.4301", "lon": "26.1001", "category": "building"}]

    metrics = run_curation(db, fetch_fn=fake_fetch)

    rows = {r[0]: r for r in db.conn.execute(
        "SELECT id, property_group_id, latitude_best, longitude_best, location_source, position_confidence FROM listings")}
    # Twin linked
    assert rows["booking_1"][1] == rows["airbnb_2"][1] is not None
    # Airbnb best position pulled toward the geocoded Booking point (was 44.4330)
    assert rows["airbnb_2"][2] < 44.4320
    assert rows["airbnb_2"][4] == "transferred_from_twin"
    assert rows["booking_1"][4] == "geocoded_address"
    assert metrics["identity_twins_grouped"] == 1


def test_run_curation_pins_radius0_airbnb_to_own_coord(db):
    # Radius-0 Airbnb (host exposed the EXACT location) + a Booking twin ~330 m
    # away. The airbnb must KEEP its own exact coord, not be averaged toward the twin.
    _mk(db, "booking_1", Platform.BOOKING, "Central Studio", 44.4300, 26.1000,
        business_phone="0721000999",
        raw_json='{"location":{"address":"5 Strada Lipscani","city":"Bucuresti"}}')
    _mk(db, "airbnb_2", Platform.AIRBNB, "Cozy Downtown", 44.4330, 26.1000,
        business_phone="+40721000999")
    db.set_airbnb_location_radius({"airbnb_2": 0.0})  # host exposes exact location
    run_curation(db, fetch_fn=lambda q: [{"lat": "44.4301", "lon": "26.1001", "category": "building"}])
    lat_best, src, prec, pp = db.conn.execute(
        "SELECT latitude_best, location_source, location_precision, platform_precision "
        "FROM listings WHERE id='airbnb_2'").fetchone()
    assert abs(lat_best - 44.4330) < 0.0005   # kept its OWN exact coord, not pulled to ~44.4301
    assert src == "platform_coord"            # not transferred_from_twin
    assert prec == "exact"
    assert pp == "exact"                      # platform_precision derived from radius 0


def test_run_curation_flags_cross_platform_disagreement(db):
    # Same unique phone (Tier-0 link) but ~8 km apart -> linked, flagged, and NOT position-transferred.
    _mk(db, "booking_1", Platform.BOOKING, "Flat", 44.4300, 26.1000, business_phone="0722000222")
    _mk(db, "airbnb_2", Platform.AIRBNB, "Flat", 44.5000, 26.1000, business_phone="+40722000222")
    metrics = run_curation(db, fetch_fn=lambda q: [])
    row = dict(zip(("gid","lat_best","src"), db.conn.execute(
        "SELECT property_group_id, latitude_best, location_source FROM listings WHERE id='airbnb_2'").fetchone()))
    assert row["gid"] is not None
    assert row["gid"] in metrics["geo_conflict_groups"]          # flagged
    assert row["lat_best"] > 44.45                                # kept its OWN ~44.50 position, NOT pulled to booking's 44.43
    assert row["src"] == "platform_coord"                         # not transferred_from_twin


def test_run_curation_discards_far_geocode(db):
    # Geocoder returns a point ~150 km away from the scraped coord -> discarded.
    _mk(db, "booking_1", Platform.BOOKING, "Flat", 44.4300, 26.1000,
        raw_json='{"basicPropertyData":{"location":{"address":"5 Strada Lipscani","city":"Bucuresti"}}}')
    run_curation(db, fetch_fn=lambda q: [{"lat": "45.5000", "lon": "24.8000", "category": "x"}])
    lat_geo, lat_best, src = db.conn.execute(
        "SELECT latitude_geocoded, latitude_best, location_source FROM listings WHERE id='booking_1'").fetchone()
    assert lat_geo is None                       # far geocode not stored
    assert abs(lat_best - 44.4300) < 0.05        # best stayed near scraped, not dragged to 45.5
    assert src == "platform_coord"


def test_run_curation_keeps_near_geocode(db):
    # Geocoder returns a point ~60 m from the scraped coord -> kept.
    _mk(db, "booking_1", Platform.BOOKING, "Flat", 44.4300, 26.1000,
        raw_json='{"basicPropertyData":{"location":{"address":"5 Strada Lipscani","city":"Bucuresti"}}}')
    run_curation(db, fetch_fn=lambda q: [{"lat": "44.4305", "lon": "26.1002", "category": "x"}])
    lat_geo, src = db.conn.execute(
        "SELECT latitude_geocoded, location_source FROM listings WHERE id='booking_1'").fetchone()
    assert lat_geo is not None                   # near geocode kept
    assert src == "geocoded_address"
