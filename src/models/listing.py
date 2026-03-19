from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime

from .enums import Platform


@dataclass
class Listing:
    id: str                         # "booking_12345" or "airbnb_67890"
    platform: Platform
    platform_id: str                # Raw platform ID
    name: str
    latitude: float
    longitude: float
    property_type: str | None = None
    star_rating: float | None = None
    review_score: float | None = None   # Normalized 0-10
    review_count: int | None = None
    price_per_night: float | None = None
    currency: str = "EUR"
    url: str = ""
    thumbnail_url: str | None = None
    bedrooms: int | None = None
    beds: int | None = None
    bathrooms: float | None = None       # float for "1.5 bath"
    max_guests: int | None = None
    is_superhost: bool | None = None    # Airbnb only
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    grid_cell_id: str = ""
    raw_json: str | None = None

    @classmethod
    def make_id(cls, platform: Platform, platform_id: str) -> str:
        return f"{platform.value}_{platform_id}"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["platform"] = self.platform.value
        d["scraped_at"] = self.scraped_at.isoformat()
        return d

    def to_row(self) -> tuple:
        """Return a tuple for SQLite insertion."""
        return (
            self.id,
            self.platform.value,
            self.platform_id,
            self.name,
            self.latitude,
            self.longitude,
            self.property_type,
            self.star_rating,
            self.review_score,
            self.review_count,
            self.price_per_night,
            self.currency,
            self.url,
            self.thumbnail_url,
            self.bedrooms,
            self.beds,
            self.bathrooms,
            self.max_guests,
            self.is_superhost,
            self.scraped_at.isoformat(),
            self.grid_cell_id,
            self.raw_json,
        )
