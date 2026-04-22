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
    business_name: str | None = None
    business_registration_number: str | None = None   # Trade register / CUI / J-number
    business_vat: str | None = None
    business_address: str | None = None
    business_email: str | None = None
    business_phone: str | None = None
    business_type: str | None = None                  # Professional / Private / Individual / Business
    business_country: str | None = None
    business_trade_register_name: str | None = None   # e.g. "ROONRC" (trade register authority)
    host_name: str | None = None                       # e.g. "Chris" / "Florin"
    host_id: str | None = None                         # platform user id
    host_response_rate: str | None = None              # e.g. "100%"
    host_response_time: str | None = None              # e.g. "Responds within an hour"
    host_join_date: str | None = None                  # e.g. "Joined in 2019"

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
            self.business_name,
            self.business_registration_number,
            self.business_vat,
            self.business_address,
            self.business_email,
            self.business_phone,
            self.business_type,
            self.business_country,
            self.business_trade_register_name,
            self.host_name,
            self.host_id,
            self.host_response_rate,
            self.host_response_time,
            self.host_join_date,
        )
