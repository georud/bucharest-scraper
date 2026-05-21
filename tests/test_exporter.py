import csv
from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing
from src.storage.exporter import export_csv, export_geojson


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


from src.storage.exporter import export_dedup_metrics


def test_export_dedup_metrics_writes_json(db, tmp_path):
    path = export_dedup_metrics({"precision_proxy": 0.97, "conflict_groups": []},
                                output_path=tmp_path / "m.json")
    import json
    assert json.load(open(path))["precision_proxy"] == 0.97
