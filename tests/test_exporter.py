import csv
from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing
from src.storage.exporter import export_csv, export_dedup_metrics, export_geojson


def _mk(db, lid, platform, lat, lng, best=None):
    db.upsert_listing(Listing(id=lid, platform=platform, platform_id=lid.split("_")[1],
                              name="X", latitude=lat, longitude=lng,
                              scraped_at=datetime(2026, 5, 15), first_seen_at=datetime(2026, 5, 15)))
    if best:
        db.conn.execute("UPDATE listings SET latitude_best=?, longitude_best=? WHERE id=?",
                        (best[0], best[1], lid))
        db.conn.commit()


def test_csv_has_new_columns(db, tmp_path):
    _mk(db, "booking_1", Platform.BOOKING, 44.43, 26.10, best=(44.4301, 26.1001))
    path = export_csv(db, output_path=tmp_path / "l.csv")
    with open(path, encoding="utf-8-sig") as f:
        header = next(csv.reader(f))
    for c in ("operator_id", "property_group_id", "latitude_best", "location_precision",
              "position_confidence"):
        assert c in header


def test_geojson_uses_best_coords_when_present(db, tmp_path):
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10, best=(44.4301, 26.1001))
    import json
    path = export_geojson(db, output_path=tmp_path / "l.geojson")
    feat = json.load(open(path, encoding="utf-8"))["features"][0]
    assert feat["geometry"]["coordinates"] == [26.1001, 44.4301]


def test_csv_map_columns_present_and_populated(db, tmp_path):
    # One curated row (best + source + precision), one uncurated (scraped only).
    _mk(db, "booking_1", Platform.BOOKING, 44.43, 26.10, best=(44.4301, 26.1001))
    db.conn.execute("UPDATE listings SET location_source='geocoded_address', "
                    "location_precision='exact' WHERE id='booking_1'")
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10)  # no best, uncurated
    db.conn.commit()

    path = export_csv(db, output_path=tmp_path / "l.csv")
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    by_id = {r["id"]: r for r in rows}

    assert {"map_latitude", "map_longitude", "map_source", "map_precision"} <= set(rows[0].keys())
    header = list(rows[0].keys())   # DictReader preserves order
    ni = header.index("name")
    assert header[ni+1:ni+5] == ["map_latitude", "map_longitude", "map_source", "map_precision"]
    assert float(by_id["booking_1"]["map_latitude"]) == 44.4301
    assert float(by_id["booking_1"]["map_longitude"]) == 26.1001
    assert by_id["booking_1"]["map_source"] == "geocoded_address"
    assert by_id["booking_1"]["map_precision"] == "exact"
    assert float(by_id["airbnb_2"]["map_latitude"]) == 44.4330
    assert by_id["airbnb_2"]["map_source"] == "platform_coord"
    assert by_id["airbnb_2"]["map_precision"] == "approximate"
    assert all(r["map_latitude"] and r["map_longitude"] for r in rows)


def test_geojson_has_map_source_and_geometry_from_map_coords(db, tmp_path):
    import json
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10, best=(44.4301, 26.1001))
    db.conn.execute("UPDATE listings SET location_source='transferred_from_twin', "
                    "location_precision='exact' WHERE id='airbnb_2'")
    db.conn.commit()
    path = export_geojson(db, output_path=tmp_path / "l.geojson")
    feat = json.load(open(path, encoding="utf-8"))["features"][0]
    assert feat["geometry"]["coordinates"] == [26.1001, 44.4301]
    assert feat["properties"]["map_source"] == "transferred_from_twin"
    assert feat["properties"]["map_precision"] == "exact"
    assert feat["properties"]["map_latitude"] == 44.4301
    assert "latitude" not in feat["properties"]


def test_export_dedup_metrics_writes_json(db, tmp_path):
    path = export_dedup_metrics({"precision_proxy": 0.97, "conflict_groups": []},
                                output_path=tmp_path / "m.json")
    import json
    assert json.load(open(path))["precision_proxy"] == 0.97
