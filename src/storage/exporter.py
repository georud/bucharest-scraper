from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from ..config import DATA_DIR
from .database import Database

logger = logging.getLogger(__name__)

EXPORTS_DIR = DATA_DIR / "exports"


def export_csv(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings to CSV."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.csv"

    rows = db.conn.execute("""
        SELECT id, platform, platform_id, name, latitude, longitude,
               property_type, star_rating, review_score, review_count,
               price_per_night, currency, url, thumbnail_url,
               bedrooms, beds, bathrooms, max_guests,
               is_superhost, scraped_at, grid_cell_id
        FROM listings ORDER BY platform, name
    """).fetchall()

    headers = [
        "id", "platform", "platform_id", "name", "latitude", "longitude",
        "property_type", "star_rating", "review_score", "review_count",
        "price_per_night", "currency", "url", "thumbnail_url",
        "bedrooms", "beds", "bathrooms", "max_guests",
        "is_superhost", "scraped_at", "grid_cell_id",
    ]

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    logger.info("Exported %d listings to %s", len(rows), path)
    return path


def export_geojson(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings as GeoJSON FeatureCollection."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.geojson"

    rows = db.conn.execute("""
        SELECT id, platform, platform_id, name, latitude, longitude,
               property_type, star_rating, review_score, review_count,
               price_per_night, currency, url, thumbnail_url,
               bedrooms, beds, bathrooms, max_guests,
               is_superhost, scraped_at
        FROM listings ORDER BY platform, name
    """).fetchall()

    features = []
    for row in rows:
        (
            lid, platform, platform_id, name, lat, lng,
            prop_type, stars, score, reviews,
            price, currency, url, thumb,
            bedrooms, beds, bathrooms, max_guests,
            superhost, scraped,
        ) = row

        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": {
                "id": lid,
                "platform": platform,
                "name": name,
                "property_type": prop_type,
                "star_rating": stars,
                "review_score": score,
                "review_count": reviews,
                "price_per_night": price,
                "currency": currency,
                "url": url,
                "thumbnail_url": thumb,
                "bedrooms": bedrooms,
                "beds": beds,
                "bathrooms": bathrooms,
                "max_guests": max_guests,
                "is_superhost": bool(superhost) if superhost is not None else None,
            },
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    logger.info("Exported %d features to %s", len(features), path)
    return path
