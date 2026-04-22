from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from ..config import DATA_DIR
from .database import Database

logger = logging.getLogger(__name__)

EXPORTS_DIR = DATA_DIR / "exports"


# Single source of truth for the columns each exporter emits, in order.
# Keep the SQL below in sync with this list when adding new Listing fields.
_EXPORT_COLUMNS = [
    "id", "platform", "platform_id", "name", "latitude", "longitude",
    "property_type", "star_rating", "review_score", "review_count",
    "price_per_night", "currency", "url", "thumbnail_url",
    "bedrooms", "beds", "bathrooms", "max_guests",
    "is_superhost", "scraped_at", "grid_cell_id",
    # Business / DSA disclosure
    "business_name", "business_registration_number", "business_vat",
    "business_address", "business_email", "business_phone",
    "business_type", "business_country", "business_trade_register_name",
    # Host profile (Airbnb)
    "host_name", "host_id", "host_response_rate", "host_response_time", "host_join_date",
]

_EXPORT_SELECT = ", ".join(_EXPORT_COLUMNS)


def export_csv(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings to CSV with every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.csv"

    rows = db.conn.execute(
        f"SELECT {_EXPORT_SELECT} FROM listings ORDER BY platform, name"
    ).fetchall()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(_EXPORT_COLUMNS)
        writer.writerows(rows)

    logger.info("Exported %d listings to %s", len(rows), path)
    return path


def export_geojson(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings as a GeoJSON FeatureCollection, including every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.geojson"

    rows = db.conn.execute(
        f"SELECT {_EXPORT_SELECT} FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "ORDER BY platform, name"
    ).fetchall()

    features = []
    for row in rows:
        props = dict(zip(_EXPORT_COLUMNS, row))
        lat = props.pop("latitude")
        lng = props.pop("longitude")
        # Coerce is_superhost to a proper boolean / None
        sh = props.get("is_superhost")
        if sh is not None:
            props["is_superhost"] = bool(sh)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": props,
        })

    geojson = {"type": "FeatureCollection", "features": features}

    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    logger.info("Exported %d features to %s", len(features), path)
    return path
