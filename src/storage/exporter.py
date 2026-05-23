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
    # Provenance + currency transparency
    "first_seen_at", "price_original", "currency_original",
    # Cross-platform linkage (same physical flat on Booking + Airbnb)
    "cross_platform_group_id",
    # Geo curation (precision + fused position)
    "operator_id", "property_group_id",
    "latitude_geocoded", "longitude_geocoded",
    "latitude_best", "longitude_best", "geocoded_address",
    "location_precision", "location_source", "est_accuracy_m", "position_confidence",
    "platform_precision", "airbnb_location_radius_m",
    # Business / DSA disclosure
    "business_name", "business_registration_number", "business_vat",
    "business_address", "business_email", "business_phone",
    "business_type", "business_country", "business_trade_register_name",
    # Host profile (Airbnb)
    "host_name", "host_id", "host_response_rate", "host_response_time", "host_join_date",
]

# Derived export columns, computed at export time (no DB schema change), keyed by
# the existing column they're inserted right after:
#   map_* — one always-populated canonical position + its source/precision, reusing
#           the COALESCE(latitude_best, latitude) rule map_builder.py plots.
#   address_raw — the FULL raw Booking property address (with bloc/scara/etaj) from
#           raw_json; the unprocessed sibling of geocoded_address (the cleaned form
#           we sent to the geocoder). NULL for Airbnb (no property address).
_DERIVED_AFTER: dict[str, list[tuple[str, str]]] = {
    "name": [
        ("map_latitude", "COALESCE(latitude_best, latitude)"),
        ("map_longitude", "COALESCE(longitude_best, longitude)"),
        ("map_source", "COALESCE(location_source, 'platform_coord')"),
        ("map_precision", "COALESCE(location_precision, 'approximate')"),
    ],
    "geocoded_address": [
        ("address_raw", "COALESCE(json_extract(raw_json, '$.basicPropertyData.location.address'), "
                        "json_extract(raw_json, '$.location.address'))"),
    ],
}


def _select_and_columns() -> tuple[str, list[str]]:
    """Return (sql_select_expr, column_names) with derived columns inserted right
    after their anchor column (see _DERIVED_AFTER). One definition shared by both
    exporters: map_* after 'name', address_raw after 'geocoded_address'."""
    names: list[str] = []
    exprs: list[str] = []
    for col in _EXPORT_COLUMNS:
        names.append(col)
        exprs.append(col)
        for dname, dexpr in _DERIVED_AFTER.get(col, []):
            names.append(dname)
            exprs.append(f"{dexpr} AS {dname}")
    return ", ".join(exprs), names


for _anchor in _DERIVED_AFTER:
    assert _anchor in _EXPORT_COLUMNS, f"_select_and_columns expects '{_anchor}' to anchor a derived column"


def export_csv(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings to CSV with every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.csv"

    select_sql, columns = _select_and_columns()
    rows = db.conn.execute(
        f"SELECT {select_sql} FROM listings ORDER BY platform, name"
    ).fetchall()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info("Exported %d listings to %s", len(rows), path)
    return path


def export_geojson(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings as a GeoJSON FeatureCollection, including every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.geojson"

    select_sql, columns = _select_and_columns()
    rows = db.conn.execute(
        f"SELECT {select_sql} FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "ORDER BY platform, name"
    ).fetchall()

    features = []
    for row in rows:
        props = dict(zip(columns, row))
        lng = props["map_longitude"]
        lat = props["map_latitude"]
        props.pop("latitude", None)
        props.pop("longitude", None)
        # Coerce is_superhost to a proper boolean / None
        sh = props.get("is_superhost")
        if sh is not None:
            props["is_superhost"] = bool(sh)
        # map_latitude/longitude kept in properties (besides geometry) for tabular consumers
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


_OPERATOR_COLUMNS = [
    "operator_key", "operator_name", "registration_number",
    "trade_register", "platforms", "listing_count", "professional_listings",
]


def export_operators_csv(db: Database, output_path: Path | None = None) -> Path:
    """Export one row per operator — listings grouped by registration number /
    business name / host id. Lets a reader answer "who controls how many
    listings" directly. See METHODOLOGY.md → Unit of analysis."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "operators.csv"

    operators = db.get_operator_summary()
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(_OPERATOR_COLUMNS)
        for op in operators:
            writer.writerow([op[c] for c in _OPERATOR_COLUMNS])

    logger.info("Exported %d operators to %s", len(operators), path)
    return path


def export_dedup_metrics(metrics: dict, output_path: Path | None = None) -> Path:
    """Write the dedup verification metrics to JSON."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "dedup_metrics.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    logger.info("Exported dedup metrics to %s", path)
    return path


def export_dedup_review(db: Database, output_path: Path | None = None, sample: int = 200) -> Path:
    """Export a reviewable sample of property groups: members, names, coords,
    distance, identity keys, photo URLs — for eyeballing dedup quality."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "dedup_review.csv"
    rows = db.conn.execute(
        """SELECT property_group_id, id, platform, name, latitude, longitude,
                  latitude_best, longitude_best, business_registration_number,
                  business_phone, thumbnail_url
           FROM listings WHERE property_group_id IS NOT NULL
           ORDER BY property_group_id LIMIT ?""", (sample * 2,)
    ).fetchall()
    header = ["property_group_id", "id", "platform", "name", "latitude", "longitude",
              "latitude_best", "longitude_best", "registration", "phone", "thumbnail_url"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    logger.info("Exported dedup review sample to %s", path)
    return path
