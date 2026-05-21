from datetime import datetime

import pytest

from src.models.enums import Platform
from src.models.listing import Listing
from src.visualization.map_builder import build_map


def test_build_map_uses_best_coords_and_shows_precision(db, tmp_path):
    db.upsert_listing(Listing(
        id="airbnb_1", platform=Platform.AIRBNB, platform_id="1", name="Test Flat",
        latitude=44.4330, longitude=26.10, scraped_at=datetime(2026, 5, 15),
        first_seen_at=datetime(2026, 5, 15)))
    db.conn.execute(
        "UPDATE listings SET latitude_best=?, longitude_best=?, location_precision=?, "
        "position_confidence=?, location_source=? WHERE id=?",
        (44.4301, 26.1001, "exact", 0.83, "geocoded_address", "airbnb_1"),
    )
    db.conn.commit()
    out = build_map(db, output_path=tmp_path / "map.html")
    html = open(out, encoding="utf-8").read()
    # Popup shows the precision line
    assert "Location" in html
    # Best coords are used for the marker (not the original scraped coords)
    assert "44.43010" in html
    assert "26.10010" in html
    # Precision value appears
    assert "exact" in html
