from __future__ import annotations

import json
import logging
from datetime import datetime

from ...models.enums import Platform
from ...models.listing import Listing
from ...text import normalize_text

logger = logging.getLogger(__name__)

def parse_graphql_results(results: list[dict], country_code: str, grid_cell_id: str = "") -> list[Listing]:
    """Parse FullSearch GraphQL results into Listing objects."""
    listings = []

    for item in results:
        try:
            listing = _parse_property(item, country_code, grid_cell_id)
            if listing:
                listings.append(listing)
        except Exception as e:
            logger.debug("Failed to parse property: %s", e)

    return listings


def _parse_property(item: dict, country_code: str, grid_cell_id: str) -> Listing | None:
    """Parse a single SearchResultProperty from the GraphQL response."""
    basic = item.get("basicPropertyData", {})
    prop_id = str(basic.get("id", ""))
    if not prop_id:
        return None

    # Coordinates
    location = basic.get("location", {})
    lat = location.get("latitude", 0.0)
    lng = location.get("longitude", 0.0)
    if lat == 0.0 and lng == 0.0:
        return None

    # Name
    name = ""
    display_name = item.get("displayName", {})
    if isinstance(display_name, dict):
        name = normalize_text(display_name.get("text", ""))

    # Star rating
    star_obj = basic.get("starRating") or {}
    star_rating = star_obj.get("value") if isinstance(star_obj, dict) else None

    # Reviews
    reviews = basic.get("reviews") or {}
    review_score = reviews.get("totalScore") if isinstance(reviews, dict) else None
    review_count = reviews.get("reviewsCount") if isinstance(reviews, dict) else None

    # Price from blocks
    price = None
    currency = "EUR"
    blocks = item.get("blocks") or []
    if blocks and isinstance(blocks, list):
        fp = blocks[0].get("finalPrice") or {}
        if isinstance(fp, dict):
            price = fp.get("amount")
            currency = fp.get("currency", "EUR")

    # Photo
    photo_url = None
    photos = basic.get("photos") or {}
    if isinstance(photos, dict):
        main = photos.get("main") or {}
        if isinstance(main, dict):
            jpeg = main.get("highResJpegUrl") or {}
            if isinstance(jpeg, dict):
                rel = jpeg.get("relativeUrl")
                if rel:
                    photo_url = f"https://cf.bstatic.com{rel}"

    # URL from pageName
    page_name = basic.get("pageName", "")
    base = f"https://www.booking.com/hotel/{country_code}"
    url = f"{base}/{page_name}.html" if page_name else f"{base}/?hotel_id={prop_id}"

    listing_id = Listing.make_id(Platform.BOOKING, prop_id)

    return Listing(
        id=listing_id,
        platform=Platform.BOOKING,
        platform_id=prop_id,
        name=name,
        latitude=lat,
        longitude=lng,
        property_type=_accommodation_type(basic.get("accommodationTypeId")),
        star_rating=star_rating,
        review_score=review_score,
        review_count=review_count,
        price_per_night=price,
        currency=currency,
        url=url,
        thumbnail_url=photo_url,
        is_superhost=None,
        scraped_at=datetime.utcnow(),
        grid_cell_id=grid_cell_id,
        raw_json=json.dumps(item, default=str),
    )


_ACCOM_TYPES = {
    201: "apartment", 202: "guest_house", 203: "hostel", 204: "hotel",
    205: "motel", 206: "resort", 208: "bed_and_breakfast", 209: "homestay",
    213: "inn", 218: "villa", 219: "chalet", 220: "holiday_home",
    222: "campsite", 223: "boat", 224: "capsule_hotel", 225: "luxury_tent",
    226: "lodge", 228: "farm_stay",
}


def _accommodation_type(type_id) -> str | None:
    if type_id is None:
        return None
    return _ACCOM_TYPES.get(int(type_id), f"type_{type_id}")
