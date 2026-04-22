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
            if currency == "RON" and price is not None:
                price = round(price / 4.97, 2)
                currency = "EUR"

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

    # Room configuration from matchingUnitConfigurations
    bedrooms = None
    beds = None
    bathrooms = None
    max_guests = None
    units = item.get("matchingUnitConfigurations")
    if units:
        # Can be a single dict or a list of dicts
        unit = units[0] if isinstance(units, list) else units
        if isinstance(unit, dict):
            common = unit.get("commonConfiguration") or {}
            if isinstance(common, dict):
                bedrooms = common.get("nbBedrooms")
                bathrooms_raw = common.get("nbBathrooms")
                bathrooms = float(bathrooms_raw) if bathrooms_raw is not None else None
                beds = common.get("nbAllBeds")
                max_guests = None

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
        bedrooms=bedrooms,
        beds=beds,
        bathrooms=bathrooms,
        max_guests=max_guests,
        is_superhost=None,
        scraped_at=datetime.utcnow(),
        grid_cell_id=grid_cell_id,
        raw_json=json.dumps(item, default=str),
    )


def parse_property_page(html: str) -> dict:
    """Extract price, room, and business/legal data from a Booking.com property page.

    Price/room extraction is tiered (JSON-LD → og:price → JS). Legal info is
    always attempted regardless of price presence, since the two live in
    different parts of the page.
    """
    result = {
        "price_per_night": None,
        "currency": None,
        "bedrooms": None,
        "beds": None,
        "bathrooms": None,
        "max_guests": None,
        "business_name": None,
        "business_registration_number": None,
        "business_vat": None,
        "business_address": None,
        "business_email": None,
        "business_phone": None,
        "business_type": None,
        "business_country": None,
        "business_trade_register_name": None,
    }

    _extract_json_ld(html, result)
    if result["price_per_night"] is None:
        _extract_meta_tags(html, result)
    if result["price_per_night"] is None:
        _extract_js_price(html, result)

    # Structured DSA disclosure (Apollo state) — sole reliable source on Booking.
    # The regex `_extract_legal_info` fallback is intentionally NOT called: it
    # matches Booking's own corporate "Legal info" footer / property geo-address
    # and produces garbage on pages that don't carry traderInfo.
    _extract_trader_info(html, result)
    return result


import re

_JSON_LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.DOTALL)
_META_PRICE_RE = re.compile(r'<meta\s+(?:property|name)="og:price:amount"\s+content="([^"]+)"', re.IGNORECASE)
_META_CURRENCY_RE = re.compile(r'<meta\s+(?:property|name)="og:price:currency"\s+content="([^"]+)"', re.IGNORECASE)
_JS_PRICE_RE = re.compile(r'"price"\s*:\s*(\d+(?:\.\d+)?)')
_JS_CURRENCY_RE = re.compile(r'"currency"\s*:\s*"([A-Z]{3})"')
_MAX_GUESTS_RE = re.compile(r'"maxOccupancy"\s*:\s*(\d+)')

# Legal-info block extraction
_LEGAL_HEADING_RE = re.compile(
    r'(legal\s+information|legal\s+info|informa[țţt]ii\s+legale|'
    r'host\s+information|company\s+information|business\s+information|'
    r'trader\s+information|partner\s+information)',
    re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_WS_RE = re.compile(r'\s+')
_VAT_RE = re.compile(
    r'(?:VAT(?:\s+(?:ID|number|no\.?))?|C\.?U\.?I\.?(?:\s+no\.?)?|Tax\s+ID|CIF|'
    r'Cod\s+fiscal|TVA)\s*[:#]?\s*([A-Z]{0,3}\s*\d[\d\s\.\-]{4,14})',
    re.IGNORECASE,
)
_ROMANIAN_J_RE = re.compile(r'\bJ\s*\d{1,3}\s*/\s*\d+\s*/\s*\d{4}\b')
_TRADE_REG_RE = re.compile(
    r'(?:trade\s+(?:register|registry)(?:\s+no\.?)?|registration\s+(?:number|no\.?)|'
    r'nr\.?\s*(?:de\s+)?(?:[îi]nregistrare|ORC)|Companies\s+House(?:\s+no\.?)?)\s*[:#]?\s*'
    r'([A-Z]?\s*\d+[/\-]?[A-Z]?\s*\d*[/\-]?\s*\d*)',
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r'[\w.+\-]+@[\w\-]+\.[\w.\-]+')
_PHONE_RE = re.compile(r'\+?\d[\d\s\.\(\)\-]{7,18}\d')
_COMPANY_NAME_RE = re.compile(
    r'(?:(?:legal(?:\s+entity)?|company|business|trader)\s*name|'
    r'denumire(?:\s+(?:societate|firm[ăa]))?|ragione\s+sociale)\s*[:#]?[ \t]*'
    r'([^\n\r]{2,120})',
    re.IGNORECASE,
)
_ADDRESS_LABEL_RE = re.compile(
    r'(?:(?:business|registered|company|trader)\s+address|adres[ăa](?:\s+sediu)?|'
    r'sediu(?:\s+social)?)\s*[:#]?\s*([^\n\r]{10,200})',
    re.IGNORECASE,
)
_HOST_TYPE_RE = re.compile(
    r'\b(professional|private\s+host|trader|business\s+host|individual\s+host|persoana\s+fizica)\b',
    re.IGNORECASE,
)


def _extract_json_ld(html: str, result: dict) -> None:
    for m in _JSON_LD_RE.finditer(html):
        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            typ = item.get("@type", "")
            if typ not in ("Hotel", "LodgingBusiness", "VacationRental", "Product", "Organization", "Corporation"):
                continue

            _apply_json_ld_business(item, result)

            if result["price_per_night"] is None:
                offers = item.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price_spec = offers.get("priceSpecification") or offers
                price = price_spec.get("price") or offers.get("price")
                if price is not None:
                    try:
                        result["price_per_night"] = float(price)
                        result["currency"] = (
                            price_spec.get("priceCurrency")
                            or offers.get("priceCurrency")
                            or "EUR"
                        )
                    except (ValueError, TypeError):
                        pass

            if result["max_guests"] is None:
                occ = item.get("occupancy") or {}
                if isinstance(occ, dict):
                    val = occ.get("value") or occ.get("maxValue")
                    if val is not None:
                        try:
                            result["max_guests"] = int(val)
                        except (ValueError, TypeError):
                            pass

            if result["bedrooms"] is None:
                num_rooms = item.get("numberOfRooms")
                if num_rooms is not None:
                    try:
                        result["bedrooms"] = int(num_rooms)
                    except (ValueError, TypeError):
                        pass


def _apply_json_ld_business(item: dict, result: dict) -> None:
    """Pull business/legal fields out of a JSON-LD Organization/Hotel/etc. item."""
    typ = item.get("@type", "")

    # legalName is the clearest signal for company/business name.
    if result["business_name"] is None:
        legal = item.get("legalName")
        if legal:
            result["business_name"] = str(legal).strip()
        elif typ in ("Organization", "Corporation"):
            nm = item.get("name")
            if nm:
                result["business_name"] = str(nm).strip()

    if result["business_vat"] is None:
        vat = item.get("vatID") or item.get("taxID")
        if vat:
            result["business_vat"] = str(vat).strip()

    # NOTE: JSON-LD address on Booking pages is the property's geographic location,
    # NOT the trader's registered business address. business_address/business_country
    # are populated exclusively from traderInfo.contactDetails in _extract_trader_info.

    if result["business_email"] is None:
        email = item.get("email")
        if email:
            result["business_email"] = str(email).strip()

    if result["business_phone"] is None:
        phone = item.get("telephone")
        if phone:
            result["business_phone"] = str(phone).strip()


def _format_ld_address(addr) -> str | None:
    if not addr:
        return None
    if isinstance(addr, str):
        return addr.strip() or None
    if isinstance(addr, dict):
        parts = [
            addr.get("streetAddress"),
            addr.get("addressLocality"),
            addr.get("postalCode"),
            addr.get("addressRegion"),
        ]
        country = addr.get("addressCountry")
        if isinstance(country, dict):
            country = country.get("name") or country.get("identifier")
        parts.append(country)
        joined = ", ".join(str(p).strip() for p in parts if p)
        return joined or None
    return None


def _extract_meta_tags(html: str, result: dict) -> None:
    m = _META_PRICE_RE.search(html)
    if m:
        try:
            result["price_per_night"] = float(m.group(1))
        except (ValueError, TypeError):
            return
        cm = _META_CURRENCY_RE.search(html)
        result["currency"] = cm.group(1) if cm else "EUR"


def _extract_js_price(html: str, result: dict) -> None:
    m = _JS_PRICE_RE.search(html)
    if m:
        try:
            result["price_per_night"] = float(m.group(1))
        except (ValueError, TypeError):
            return
        cm = _JS_CURRENCY_RE.search(html)
        result["currency"] = cm.group(1) if cm else "EUR"

    gm = _MAX_GUESTS_RE.search(html)
    if gm:
        result["max_guests"] = int(gm.group(1))


def _extract_legal_info(html: str, result: dict) -> None:
    """Parse a 'Legal Information' / 'Trader information' block from the property page.

    Booking renders the DSA-mandated business disclosure as an inline section.
    Layout and class names vary, so we locate it by heading text and then
    regex against the cleaned block text.
    """
    m = _LEGAL_HEADING_RE.search(html)
    if not m:
        return

    # Grab a generous window after the heading; tags get stripped below.
    start = m.end()
    chunk = html[start:start + 4000]
    # Insert newlines at block-tag boundaries so label/value pairs stay on
    # separate lines once tags are stripped.
    chunk = re.sub(r"<\s*br\s*/?>", "\n", chunk, flags=re.IGNORECASE)
    chunk = re.sub(
        r"</\s*(p|div|li|dd|dt|tr|section|h[1-6])\s*>",
        "\n",
        chunk,
        flags=re.IGNORECASE,
    )
    stripped = _HTML_TAG_RE.sub(" ", chunk)
    # Normalize spaces but preserve newlines
    stripped = re.sub(r"[ \t]+", " ", stripped)
    text = "\n".join(ln.strip() for ln in stripped.splitlines() if ln.strip())
    if not text:
        return

    if result["business_vat"] is None:
        mm = _VAT_RE.search(text)
        if mm:
            raw = re.sub(r"\s+", "", mm.group(1)).strip(".-")
            if raw:
                result["business_vat"] = raw

    if result["business_registration_number"] is None:
        rm = _ROMANIAN_J_RE.search(text)
        if rm:
            result["business_registration_number"] = re.sub(r"\s+", "", rm.group(0))
        else:
            tm = _TRADE_REG_RE.search(text)
            if tm:
                result["business_registration_number"] = tm.group(1).strip()

    if result["business_email"] is None:
        em = _EMAIL_RE.search(text)
        if em:
            result["business_email"] = em.group(0).strip().rstrip(".,;:")

    if result["business_phone"] is None:
        for pm in _PHONE_RE.finditer(text):
            candidate = pm.group(0).strip()
            digits = re.sub(r"\D", "", candidate)
            if 8 <= len(digits) <= 15:
                result["business_phone"] = candidate
                break

    if result["business_name"] is None:
        nm = _COMPANY_NAME_RE.search(text)
        if nm:
            result["business_name"] = nm.group(1).strip().rstrip(".,;:").strip()

    if result["business_address"] is None:
        am = _ADDRESS_LABEL_RE.search(text)
        if am:
            result["business_address"] = am.group(1).strip().rstrip(".,;:").strip()

    if result["business_type"] is None:
        tm = _HOST_TYPE_RE.search(text)
        if tm:
            label = tm.group(1).strip().title()
            result["business_type"] = label
        elif (
            result["business_name"]
            or result["business_vat"]
            or result["business_registration_number"]
        ):
            result["business_type"] = "Professional"


def _extract_trader_info(html: str, result: dict) -> None:
    """Extract Booking's structured DSA disclosure from the page's Apollo state.

    Booking embeds it as ``"legalInfo":{"__typename":"LegalInfo","traderInfo":{...}}``
    inside the rendered page. Parsing the JSON object directly is far more
    reliable than regex against rendered text.
    """
    idx = html.find('"traderInfo"')
    if idx < 0:
        return

    brace_start = html.find('{', idx)
    if brace_start < 0:
        return

    # Walk forward, counting braces while respecting JSON strings + escapes,
    # to extract the balanced traderInfo object.
    depth = 0
    in_str = False
    escape = False
    end = -1
    limit = min(brace_start + 20_000, len(html))
    for i in range(brace_start, limit):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return

    blob = html[brace_start:end + 1]
    try:
        data = json.loads(blob)
    except (json.JSONDecodeError, ValueError):
        return

    is_trader = data.get("isTrader")
    if is_trader is False:
        if result["business_type"] is None:
            result["business_type"] = "Private"
        return

    contact = data.get("contactDetails") or {}

    if result["business_name"] is None:
        name = contact.get("companyLegalName")
        if not name:
            parts = [contact.get(k) for k in ("firstName", "middleName", "lastName")]
            name = " ".join(p for p in parts if p)
        if name:
            result["business_name"] = name.strip()

    if result["business_registration_number"] is None:
        reg = contact.get("registrationNumber")
        if reg:
            result["business_registration_number"] = str(reg).strip()

    if result["business_trade_register_name"] is None:
        tr = contact.get("tradeRegisterName")
        if tr:
            result["business_trade_register_name"] = str(tr).strip()

    if result["business_email"] is None:
        em = contact.get("email")
        if em:
            result["business_email"] = str(em).strip()

    if result["business_phone"] is None:
        ph = contact.get("phoneNumber")
        if ph:
            result["business_phone"] = str(ph).strip()

    if result["business_address"] is None:
        addr = contact.get("address") or {}
        parts = [
            addr.get("addressLine1"),
            addr.get("addressLine2"),
            addr.get("city"),
            addr.get("postalCode"),
            addr.get("state"),
        ]
        country = addr.get("countryCode")
        if country and isinstance(country, str):
            country_up = country.upper()
            parts.append(country_up)
            if result["business_country"] is None:
                result["business_country"] = country_up
        joined = ", ".join(
            p.strip() for p in parts
            if p and isinstance(p, str) and p.strip()
        )
        if joined:
            result["business_address"] = joined

    if result["business_type"] is None:
        rst = data.get("regulatorySubjectType")
        if rst == "BUSINESS":
            result["business_type"] = "Professional"
        elif rst in ("PRIVATE_INDIVIDUAL", "PRIVATE"):
            result["business_type"] = "Individual"
        else:
            # isTrader was True (or unknown) — default to Professional
            result["business_type"] = "Professional"


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
