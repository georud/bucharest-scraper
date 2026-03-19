from __future__ import annotations

import json
import logging
import re
from datetime import datetime

from ...models.enums import Platform
from ...models.listing import Listing
from ...text import normalize_text

logger = logging.getLogger(__name__)


def _get_nested(d: dict, path: str, default=None):
    """Navigate a nested dict via dotted path, e.g. 'a.b.c'."""
    current = d
    for key in path.split("."):
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def parse_raw_api_results(raw_json: dict, grid_cell_id: str = "") -> list[Listing]:
    """Parse raw Airbnb StaysSearch API JSON directly, bypassing pyairbnb's standardize.

    This avoids the `case _: continue` bug in pyairbnb's from_search() that silently
    drops listings whose secondary price string has 4+ words.
    """
    listings = []

    results = _get_nested(raw_json, "data.presentation.staysSearch.results.searchResults")
    if not results:
        return listings

    for result in results:
        try:
            if _get_nested(result, "__typename") != "StaySearchResult":
                continue

            listing = _parse_raw_result(result, grid_cell_id)
            if listing:
                listings.append(listing)
        except Exception as e:
            logger.debug("Failed to parse raw Airbnb result: %s", e)

    return listings


def extract_pagination_cursor(raw_json: dict) -> str | None:
    """Extract the next page cursor from raw StaysSearch response."""
    return _get_nested(raw_json, "data.presentation.staysSearch.results.paginationInfo.nextPageCursor")


def _parse_raw_result(result: dict, grid_cell_id: str) -> Listing | None:
    """Parse a single raw StaySearchResult into a Listing."""
    from pyairbnb.standardize import decode_listing_id

    # Room ID via base64 decode
    raw_id = _get_nested(result, "demandStayListing.id", "")
    prop_id = str(decode_listing_id(raw_id))
    if not prop_id or prop_id == "0":
        return None

    # Coordinates
    lat = _get_nested(result, "demandStayListing.location.coordinate.latitude", 0.0)
    lng = _get_nested(result, "demandStayListing.location.coordinate.longitude", 0.0)
    if lat == 0.0 and lng == 0.0:
        return None

    # Name
    name = normalize_text(
        _get_nested(result, "demandStayListing.description.name.localizedStringWithTranslationPreference", "")
    )

    # Rating — "4.85 (120)" format, graceful split
    review_score = None
    review_count = None
    avg_rating_str = _get_nested(result, "avgRatingLocalized", "")
    if avg_rating_str:
        parts = avg_rating_str.split(" ", 1)
        try:
            review_score = float(parts[0].replace(",", "."))
        except (ValueError, IndexError):
            pass
        if len(parts) > 1:
            m = re.search(r"\d+", parts[1])
            if m:
                review_count = int(m.group())

    # Price — primary line only, no secondary price dependency
    price = None
    currency = "EUR"
    pr = _get_nested(result, "structuredDisplayPrice", {})
    price_str = _get_nested(pr, "primaryLine.originalPrice", "")
    if not price_str:
        price_str = _get_nested(pr, "primaryLine.price", "")
    if price_str:
        digits = re.findall(r"\d+", price_str.replace(",", ""))
        if digits:
            price = float("".join(digits))

    # Room info from structuredContent.mapPrimaryLine
    room_info = {"bedrooms": None, "beds": None, "bathrooms": None, "max_guests": None}
    sc = _get_nested(result, "structuredContent", {})
    if isinstance(sc, dict):
        primary_line = sc.get("mapPrimaryLine", [])
        if isinstance(primary_line, list):
            texts = [
                e.get("body", "") if isinstance(e, dict) else str(e)
                for e in primary_line
            ]
            room_info = _parse_room_text(texts)

    # Photo
    context_photos = _get_nested(result, "contextualPictures", [])
    photo_url = None
    if context_photos and isinstance(context_photos, list):
        photo_url = _get_nested(context_photos[0], "picture")

    listing_id = Listing.make_id(Platform.AIRBNB, prop_id)

    return Listing(
        id=listing_id,
        platform=Platform.AIRBNB,
        platform_id=prop_id,
        name=name,
        latitude=float(lat),
        longitude=float(lng),
        property_type=None,
        star_rating=None,
        review_score=review_score,
        review_count=review_count,
        price_per_night=price,
        currency=currency,
        url=f"https://www.airbnb.com/rooms/{prop_id}",
        thumbnail_url=photo_url,
        bedrooms=room_info["bedrooms"],
        beds=room_info["beds"],
        bathrooms=room_info["bathrooms"],
        max_guests=room_info["max_guests"],
        is_superhost=None,
        scraped_at=datetime.utcnow(),
        grid_cell_id=grid_cell_id,
        raw_json=json.dumps(result, default=str),
    )


def parse_airbnb_results(data: dict, grid_cell_id: str = "") -> list[Listing]:
    """Parse Airbnb StaysSearch API response into Listing objects."""
    listings = []

    try:
        results = data["data"]["presentation"]["staysSearch"]["mapResults"]["mapSearchResults"]
    except (KeyError, TypeError):
        logger.debug("Skipping non-search Airbnb response (missing mapSearchResults)")
        return listings

    for item in results:
        try:
            listing = _parse_airbnb_item(item, grid_cell_id)
            if listing:
                listings.append(listing)
        except Exception as e:
            logger.debug("Failed to parse Airbnb item: %s", e)

    return listings


def parse_pyairbnb_results(results: list[dict], grid_cell_id: str = "") -> list[Listing]:
    """Parse pyairbnb library search results into Listing objects."""
    listings = []

    for item in results:
        try:
            listing = _parse_pyairbnb_item(item, grid_cell_id)
            if listing:
                listings.append(listing)
        except Exception as e:
            logger.debug("Failed to parse pyairbnb item: %s", e)

    return listings


def _parse_room_text(texts: list[str]) -> dict:
    """Extract bedroom/bed/bathroom/guest counts from structured text lines.

    Lines look like: "2 bedrooms", "3 beds", "1 bathroom", "4 guests".
    """
    info: dict = {"bedrooms": None, "beds": None, "bathrooms": None, "max_guests": None}
    for text in texts:
        t = text.lower().strip()
        m = re.search(r"(\d+\.?\d*)\s*bedroom", t)
        if m:
            info["bedrooms"] = int(float(m.group(1)))
        m = re.search(r"(\d+\.?\d*)\s*bed(?!room)", t)
        if m:
            info["beds"] = int(float(m.group(1)))
        m = re.search(r"(\d+\.?\d*)\s*bath", t)
        if m:
            info["bathrooms"] = float(m.group(1))
        m = re.search(r"(\d+)\s*guest", t)
        if m:
            info["max_guests"] = int(m.group(1))
        if "studio" in t:
            info["bedrooms"] = info["bedrooms"] or 0
    return info


def _parse_airbnb_item(item: dict, grid_cell_id: str) -> Listing | None:
    """Parse a single Airbnb map search result."""
    listing_data = item.get("listing", {})
    prop_id = str(listing_data.get("id", ""))
    if not prop_id:
        return None

    coordinate = listing_data.get("coordinate", {})
    lat = coordinate.get("latitude", 0.0)
    lng = coordinate.get("longitude", 0.0)

    if lat == 0.0 and lng == 0.0:
        return None

    name = normalize_text(listing_data.get("name", ""))
    room_type = listing_data.get("roomTypeCategory", "")

    # Rating
    avg_rating = listing_data.get("avgRating")
    reviews_count = listing_data.get("reviewsCount")

    # Price
    pricing = item.get("pricingQuote", {})
    price = None
    currency = "USD"
    rate = pricing.get("rate", {})
    if rate:
        amount_obj = rate.get("amount")
        if amount_obj is not None:
            price = float(amount_obj)
        currency = rate.get("currency", "USD")

    # Room details from structuredContent
    room_info = {"bedrooms": None, "beds": None, "bathrooms": None, "max_guests": None}
    sc = listing_data.get("structuredContent", {})
    if isinstance(sc, dict):
        primary_line = sc.get("mapPrimaryLine", [])
        if isinstance(primary_line, list):
            texts = [
                e.get("body", "") if isinstance(e, dict) else str(e)
                for e in primary_line
            ]
            room_info = _parse_room_text(texts)

    # Superhost
    is_superhost = listing_data.get("isSuperhost", False)

    # Photo
    context_photo = listing_data.get("contextualPictures", [])
    photo_url = context_photo[0].get("picture") if context_photo else None

    listing_id = Listing.make_id(Platform.AIRBNB, prop_id)

    return Listing(
        id=listing_id,
        platform=Platform.AIRBNB,
        platform_id=prop_id,
        name=name,
        latitude=lat,
        longitude=lng,
        property_type=room_type,
        star_rating=None,
        review_score=avg_rating,
        review_count=reviews_count,
        price_per_night=price,
        currency=currency,
        url=f"https://www.airbnb.com/rooms/{prop_id}",
        thumbnail_url=photo_url,
        bedrooms=room_info["bedrooms"],
        beds=room_info["beds"],
        bathrooms=room_info["bathrooms"],
        max_guests=room_info["max_guests"],
        is_superhost=is_superhost,
        scraped_at=datetime.utcnow(),
        grid_cell_id=grid_cell_id,
        raw_json=json.dumps(item, default=str),
    )


def _parse_pyairbnb_item(item: dict, grid_cell_id: str) -> Listing | None:
    """Parse a single pyairbnb search result dict.

    pyairbnb returns nested dicts with structure:
        room_id, name, coordinates{latitude, longitud}, rating{value, reviewCount},
        price{unit{amount, currency}}, images[...]
    Note: pyairbnb has a typo — "longitud" not "longitude".
    """
    prop_id = str(item.get("room_id", ""))
    if not prop_id:
        return None

    coords = item.get("coordinates", {})
    lat = coords.get("latitude", 0.0)
    # pyairbnb typo: "longitud" instead of "longitude"
    lng = coords.get("longitud") or coords.get("longitude", 0.0)

    if lat == 0.0 and lng == 0.0:
        return None

    name = normalize_text(item.get("name", ""))

    # Price is nested: price.unit.amount / price.unit.currency
    price_obj = item.get("price", {})
    unit = price_obj.get("unit", {}) if isinstance(price_obj, dict) else {}
    price = unit.get("amount")
    currency = unit.get("currency", "USD")

    # Rating is nested: rating.value / rating.reviewCount (string)
    rating_obj = item.get("rating", {})
    review_score = rating_obj.get("value") if isinstance(rating_obj, dict) else None
    review_count_raw = rating_obj.get("reviewCount") if isinstance(rating_obj, dict) else None
    review_count = int(review_count_raw) if review_count_raw else None

    # Images list
    images = item.get("images", [])
    thumbnail_url = images[0] if images and isinstance(images[0], str) else None

    # Room details from structuredContent.mapPrimaryLine
    room_info = {"bedrooms": None, "beds": None, "bathrooms": None, "max_guests": None}
    sc = item.get("structuredContent", {})
    if isinstance(sc, dict):
        primary_line = sc.get("mapPrimaryLine", [])
        if isinstance(primary_line, list):
            texts = [
                e.get("body", "") if isinstance(e, dict) else str(e)
                for e in primary_line
            ]
            room_info = _parse_room_text(texts)

    listing_id = Listing.make_id(Platform.AIRBNB, prop_id)

    return Listing(
        id=listing_id,
        platform=Platform.AIRBNB,
        platform_id=prop_id,
        name=name,
        latitude=float(lat),
        longitude=float(lng),
        property_type=item.get("room_type") or item.get("type"),
        star_rating=None,
        review_score=review_score,
        review_count=review_count,
        price_per_night=float(price) if price is not None else None,
        currency=currency,
        url=f"https://www.airbnb.com/rooms/{prop_id}",
        thumbnail_url=thumbnail_url,
        bedrooms=room_info["bedrooms"],
        beds=room_info["beds"],
        bathrooms=room_info["bathrooms"],
        max_guests=room_info["max_guests"],
        is_superhost=item.get("is_superhost"),
        scraped_at=datetime.utcnow(),
        grid_cell_id=grid_cell_id,
        raw_json=json.dumps(item, default=str),
    )
