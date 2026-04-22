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

    # Price — prefer per-night rate from explanationData over total
    price = None
    currency = "EUR"
    pr = _get_nested(result, "structuredDisplayPrice", {})

    # Try to extract nightly rate from priceDetails: "5 nights x € 174.69"
    price_details = _get_nested(pr, "explanationData.priceDetails") or []
    for detail in price_details:
        if not isinstance(detail, dict):
            continue
        items = detail.get("items") or []
        for item in items:
            desc = item.get("description", "") if isinstance(item, dict) else ""
            m = re.search(r"(\d+)\s*nights?\s*[x×]\s*[^\d]*(\d[\d.,]*)", desc)
            if m:
                price = float(m.group(2).replace(",", "."))
                break
        if price is not None:
            break

    # Fallback to primary line (may be a total)
    if price is None:
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


def parse_detail_response(detail: dict) -> dict:
    """Extract room fields from pyairbnb.get_details() response.

    Returns dict with keys: max_guests, is_superhost, bedrooms, beds, bathrooms.
    """
    result: dict = {
        "max_guests": None,
        "is_superhost": None,
        "bedrooms": None,
        "beds": None,
        "bathrooms": None,
    }

    # person_capacity → max_guests
    cap = detail.get("person_capacity")
    if cap is not None:
        result["max_guests"] = int(cap)

    # is_super_host → is_superhost
    superhost = detail.get("is_super_host")
    if superhost is not None:
        result["is_superhost"] = bool(superhost)

    # sub_description.items: ["4 guests", "1 bedroom", "2 beds", "1 bath"]
    sub_desc = detail.get("sub_description") or {}
    items = sub_desc.get("items") or [] if isinstance(sub_desc, dict) else []
    if items:
        room_info = _parse_room_text(items)
        for key in ("bedrooms", "beds", "bathrooms"):
            if room_info[key] is not None:
                result[key] = room_info[key]
        if result["max_guests"] is None and room_info["max_guests"] is not None:
            result["max_guests"] = room_info["max_guests"]

    return result


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


def _extract_balanced_json_object(s: str, start_brace: int) -> str | None:
    """Return the string `s[start_brace:end]` containing one balanced JSON object.

    `start_brace` must be the index of the opening `{`. Respects JSON string
    quoting and escape sequences.
    """
    depth = 0
    in_str = False
    escape = False
    for i in range(start_brace, min(len(s), start_brace + 30_000)):
        c = s[i]
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
                return s[start_brace:i + 1]
    return None


def _parse_json_at_key(html: str, key: str) -> tuple[str | None, dict | None]:
    """Find `"key":<value>` in html. Returns (literal_value, parsed_object).

    - If the value is `null`, returns ("null", None).
    - If the value is an object `{...}`, returns ("object", dict).
    - Otherwise returns (None, None) — key not found or value not interesting.
    """
    needle = f'"{key}"'
    idx = html.find(needle)
    if idx < 0:
        return (None, None)
    after = html[idx + len(needle):]
    after = after.lstrip(" \t:")
    if after.startswith("null"):
        return ("null", None)
    if after.startswith("{"):
        blob = _extract_balanced_json_object(after, 0)
        if blob:
            try:
                return ("object", json.loads(blob))
            except (json.JSONDecodeError, ValueError):
                return ("object", None)
    return (None, None)


_TAG_STRIP_RE = re.compile(r"<[^>]+>")


def _unescape_json_string(s: str) -> str:
    """Unescape JSON-embedded string: \\u00xx, \\n, \\", \\/ etc."""
    try:
        return json.loads('"' + s + '"')
    except Exception:
        return s.replace("\\u003c", "<").replace("\\u003e", ">").replace('\\"', '"').replace("\\/", "/").replace("\\n", " ")


def _extract_dsa_disclosure_html(html: str) -> str | None:
    """Locate the Professional Host DSA disclosure text embedded in the PDP page.

    Structure (for Professional listings):
        "html": {"__typename": "Html",
                 "htmlText": "$FIRSTNAME is solely responsible for offering this
                              listing on Airbnb, and they host as a business. ...
                              <ul><li>Business name: ...</li>
                                  <li>Business registry: ...</li>
                                  <li>Unique identification number: ...</li>
                                  <li>Email: ...</li>
                                  <li>Phone: ...</li>
                                  <li>Address: ...</li></ul> ..."}
    """
    anchor = "is solely responsible for offering this listing on Airbnb, and they host as a business"
    idx = html.find(anchor)
    if idx < 0:
        return None
    # Walk backwards to the opening quote of htmlText
    start = html.rfind('"htmlText":"', max(0, idx - 500), idx)
    if start < 0:
        return None
    start += len('"htmlText":"')
    # Walk forward from idx to the closing quote of htmlText (respecting JSON escapes)
    escape = False
    for i in range(idx, min(len(html), idx + 4000)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            return html[start:i]
    return None


def _parse_dsa_disclosure(html_text: str) -> dict:
    """Pull name/registry/UIN/email/phone/address from the inline DSA HTML blob."""
    text = _unescape_json_string(html_text)
    # Strip HTML tags while keeping `<li>`-boundary newlines
    text = re.sub(r"<\s*br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</\s*li\s*>", "\n", text, flags=re.IGNORECASE)
    text = _TAG_STRIP_RE.sub(" ", text)
    text = re.sub(r"[ \t]+", " ", text).strip()

    fields = {
        "business_name": None,
        "business_registration_number": None,
        "business_trade_register_name": None,
        "business_email": None,
        "business_phone": None,
        "business_address": None,
        "business_country": None,
    }

    def grab(regex: str) -> str | None:
        m = re.search(regex, text, flags=re.IGNORECASE)
        if not m:
            return None
        val = m.group(1).strip().rstrip(".,;:").strip()
        return val or None

    fields["business_name"] = grab(r"Business name\s*:\s*([^\n]+)")
    fields["business_trade_register_name"] = grab(r"Business registry\s*:\s*([^\n]+)")
    fields["business_registration_number"] = (
        grab(r"Unique identification number\s*:\s*([^\n]+)")
        or grab(r"Registration number\s*:\s*([^\n]+)")
    )
    fields["business_email"] = grab(r"Email\s*:\s*([^\n]+)")
    fields["business_phone"] = grab(r"Phone\s*:\s*([^\n]+)")
    fields["business_address"] = grab(r"Address\s*:\s*([^\n]+)")

    # Derive country from address suffix (works for the common ", Romania" pattern)
    addr = fields["business_address"]
    if addr:
        last_piece = addr.rsplit(",", 1)[-1].strip()
        if last_piece and 2 <= len(last_piece) <= 60 and last_piece[0].isalpha():
            fields["business_country"] = last_piece

    return fields


_HOST_ID_RE = re.compile(r'"PassportCardData"[^}]*?"name"\s*:\s*"([^"\\]{1,80})"[^}]*?"userId"\s*:\s*"([^"\\]{1,120})"')
_HOSTED_BY_RE = re.compile(r'"title"\s*:\s*"Hosted by ([^"\\]{1,80})"')
_SUPERHOST_TITLE_RE = re.compile(r'"superhostTitleText"\s*:\s*"([A-Z][\w \-\.]{0,60}?) is a Superhost"')
_HOST_DETAILS_ARRAY_RE = re.compile(r'"hostDetails"\s*:\s*\[([^\]]{0,500})\]')
# Airbnb exposes host tenure as strings like "7 years hosting" or "4 years on Airbnb"
# (prior field name "joinedString" is no longer populated on current PDP responses).
_YEARS_HOSTING_RE = re.compile(
    r'"title"\s*:\s*"(\d+\s*years?\s*(?:hosting|on\s+Airbnb))"',
    re.IGNORECASE,
)
_JOINED_STRING_RE = re.compile(r'"joinedString"\s*:\s*"([^"\\]{1,80})"')


def _extract_host_meta(html: str) -> dict:
    """Extract host name, user id, response rate/time, join string from Apollo state."""
    out = {
        "host_name": None,
        "host_id": None,
        "host_response_rate": None,
        "host_response_time": None,
        "host_join_date": None,
    }

    # Preferred: PassportCardData {name, userId} — present for both individual & pro
    m = _HOST_ID_RE.search(html)
    if m:
        out["host_name"] = m.group(1).strip()
        uid = m.group(2).strip()
        # Airbnb often base64-encodes the GQL id. Try to recover the numeric id for readability.
        try:
            import base64
            decoded = base64.b64decode(uid + "==", validate=False).decode("utf-8", errors="ignore")
            num_match = re.search(r"\d{3,}", decoded)
            out["host_id"] = num_match.group(0) if num_match else uid
        except Exception:
            out["host_id"] = uid

    # Fallbacks for host_name
    if not out["host_name"]:
        m = _HOSTED_BY_RE.search(html)
        if m:
            out["host_name"] = m.group(1).strip()
    if not out["host_name"]:
        m = _SUPERHOST_TITLE_RE.search(html)
        if m:
            out["host_name"] = m.group(1).strip()

    # Response rate / time — hostDetails array is ["Response rate: 100%", "Responds within an hour"]
    m = _HOST_DETAILS_ARRAY_RE.search(html)
    if m:
        items = re.findall(r'"([^"\\]{1,120})"', m.group(1))
        for item in items:
            low = item.lower()
            if out["host_response_rate"] is None and "response rate" in low:
                out["host_response_rate"] = item.strip()
            if out["host_response_time"] is None and ("respond" in low and "response rate" not in low):
                out["host_response_time"] = item.strip()

    m = _YEARS_HOSTING_RE.search(html)
    if m:
        out["host_join_date"] = m.group(1).strip()
    else:
        m = _JOINED_STRING_RE.search(html)
        if m:
            out["host_join_date"] = m.group(1).strip()

    return out


def parse_airbnb_business_from_html(html: str) -> dict:
    """Classify an Airbnb listing as Professional/Individual from the page Apollo state.

    Priority of signals (first non-null wins for classification, and any
    disclosure text that is exposed is captured as `business_name`):

      1. `"hostLegalDisclaimer":"..."` non-null string → Professional, stash text.
      2. `"businessDetails":{...}` where `.detailsHtml` or `.title` is non-null
         → Professional, stash text (stripped of HTML).
      3. `"businessDetailsItem":{...}` present with non-null title (the "This
         listing is offered by a business. Learn more" card) → Professional,
         no business_name (Airbnb doesn't expose the company details in the
         headless-rendered page state).
      4. `"businessDetails":null` AND no `businessDetailsItem` → Individual.
      5. No signals → return business_type=None (retry next run).
    """
    result = {
        "business_name": None,
        "business_registration_number": None,
        "business_vat": None,
        "business_address": None,
        "business_email": None,
        "business_phone": None,
        "business_type": None,
        "business_country": None,
        "business_trade_register_name": None,
        "host_name": None,
        "host_id": None,
        "host_response_rate": None,
        "host_response_time": None,
        "host_join_date": None,
    }
    if not html:
        return result

    # Host metadata (name, id, response rate, etc.) — populated regardless of
    # Professional/Individual classification.
    result.update({k: v for k, v in _extract_host_meta(html).items() if v is not None})

    # Professional DSA disclosure text — if present, this carries the full
    # business contact block (name, registry, UIN, email, phone, address).
    disclosure = _extract_dsa_disclosure_html(html)
    if disclosure:
        for k, v in _parse_dsa_disclosure(disclosure).items():
            if v is not None:
                result[k] = v
        if result["business_name"] or result["business_registration_number"]:
            result["business_type"] = "Professional"
            return result

    # --- 1. hostLegalDisclaimer ------------------------------------------------
    # Fast substring check — the field can appear in multiple places in the
    # Apollo state. The first non-null wins.
    for m in re.finditer(r'"hostLegalDisclaimer"\s*:\s*"((?:[^"\\]|\\.){1,2000})"', html):
        raw = m.group(1).encode("utf-8").decode("unicode_escape", errors="replace").strip()
        if raw:
            result["business_name"] = raw[:500]
            result["business_type"] = "Professional"
            return result

    # --- 2. businessDetails object with real fields ---------------------------
    kind, bd = _parse_json_at_key(html, "businessDetails")
    if kind == "object" and isinstance(bd, dict):
        details_html = bd.get("detailsHtml")
        title = bd.get("title")
        disclaimer = bd.get("hostLegalDisclaimer")
        text = None
        for candidate in (details_html, title, disclaimer):
            if isinstance(candidate, str) and candidate.strip():
                text = candidate.strip()
                break
        if text:
            # Strip HTML tags from detailsHtml if any
            clean = _TAG_STRIP_RE.sub(" ", text)
            clean = re.sub(r"\s+", " ", clean).strip()
            if clean:
                result["business_name"] = clean[:500]
                result["business_type"] = "Professional"
                return result

    # --- 3. businessDetailsItem — classify on action.screenId --------------
    # Airbnb renders this entry-point item on BOTH individual and professional
    # listings. The screenId discriminates:
    #   "PROFESSIONAL_HOST_DETAILS" → real trader
    #   "INDIVIDUAL_HOST_PROMPT"    → individual host (DSA explanation modal)
    # Title-text fallback guards against future screenId renames.
    kind, item = _parse_json_at_key(html, "businessDetailsItem")
    if kind == "object" and isinstance(item, dict):
        action = item.get("action") or {}
        screen_id = action.get("screenId") if isinstance(action, dict) else None
        title = item.get("title") or ""
        title_lower = title.lower()

        if screen_id == "PROFESSIONAL_HOST_DETAILS" or "offered by a business" in title_lower:
            result["business_type"] = "Professional"
            return result
        if screen_id == "INDIVIDUAL_HOST_PROMPT" or "offered by an individual" in title_lower:
            result["business_type"] = "Individual"
            return result
        # Unknown screenId — fall through to the null fallback below

    # --- 4. Individual fallback -----------------------------------------------
    if kind == "null" or (kind is None and '"businessDetails"' in html):
        # We saw the schema field but nothing indicates a trader.
        result["business_type"] = "Individual"
        return result

    # Fallback to the explicit null case caught above; also catch when we saw
    # hostLegalDisclaimer: null earlier in the document.
    if '"businessDetails":null' in html:
        result["business_type"] = "Individual"
        return result

    # --- 5. Nothing matched → retry later -------------------------------------
    return result


_BUSINESS_LABELS = {
    "business_name": [
        "business name", "legal name", "trader name", "company name",
        "denumire societate", "denumire firma", "denumire firmă", "denumire",
        "ragione sociale",
    ],
    "business_registration_number": [
        "trade register number", "trade register", "registration number",
        "business registration", "commerce register",
        "nr. ordine registrul comertului", "nr. de inregistrare", "nr. inregistrare",
    ],
    "business_vat": [
        "vat id", "vat number", "vat", "tax id", "tax identification number",
        "c.u.i.", "cui", "cif", "cod fiscal", "cod unic de inregistrare",
    ],
    "business_address": [
        "business address", "registered address", "trader address", "address",
        "adresa", "adresă", "sediu social", "sediu",
    ],
    "business_email": [
        "email address", "e-mail address", "email", "contact email", "e-mail",
    ],
    "business_phone": [
        "phone number", "telephone number", "contact phone", "telephone", "phone",
    ],
    "business_country": [
        "country", "tara", "țara",
    ],
}

_BUSINESS_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BUSINESS_WS_RE = re.compile(r"[ \t]+")
_EMAIL_FALLBACK_RE = re.compile(r"[\w.+\-]+@[\w\-]+\.[\w.\-]+")
_PHONE_FALLBACK_RE = re.compile(r"\+?\d[\d\s\.\(\)\-]{7,18}\d")


def parse_business_modal(source) -> dict:
    """Extract host/business disclosure fields from Airbnb's 'Learn more' modal.

    Accepts the modal's inner HTML (string) or its already-extracted inner text.
    The modal is rendered as label/value pairs (dt/dd, h-label+p, or similar).
    Matching is by label text (EN + RO) so minor DOM variations don't break it.

    Always returns a dict with all `business_*` keys. `business_type` is set
    on every non-empty invocation so callers can distinguish 'never attempted'
    from 'attempted, nothing found'.
    """
    result = {
        "business_name": None,
        "business_registration_number": None,
        "business_vat": None,
        "business_address": None,
        "business_email": None,
        "business_phone": None,
        "business_type": None,
        "business_country": None,
    }
    if not source:
        return result

    raw = source if isinstance(source, str) else str(source)

    # Convert <br>, </p>, </div> to newlines then strip remaining tags — preserves
    # label/value line breaks that Airbnb uses inside the modal.
    text = re.sub(r"<\s*br\s*/?>", "\n", raw, flags=re.IGNORECASE)
    text = re.sub(r"</\s*(p|div|li|dd|dt|tr|section)\s*>", "\n", text, flags=re.IGNORECASE)
    text = _BUSINESS_HTML_TAG_RE.sub(" ", text)
    # Normalize multiple spaces but keep newlines (label/value separators).
    text = _BUSINESS_WS_RE.sub(" ", text)
    lines = [ln.strip(" \t:-••") for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    if not lines:
        return result

    lowered = [ln.lower() for ln in lines]

    for key, labels in _BUSINESS_LABELS.items():
        if result[key] is not None:
            continue
        # Build a word-boundary regex per label list. Longest labels first so
        # "phone number" wins over "phone" when both match.
        sorted_labels = sorted(labels, key=len, reverse=True)
        label_res = [
            (label, re.compile(r"(?<![a-z0-9])" + re.escape(label) + r"(?![a-z0-9])"))
            for label in sorted_labels
        ]

        for i, ln_lc in enumerate(lowered):
            hit_label = None
            hit_match = None
            for label, pattern in label_res:
                m_lbl = pattern.search(ln_lc)
                if m_lbl:
                    hit_label = label
                    hit_match = m_lbl
                    break
            if hit_label is None:
                continue

            orig = lines[i]
            idx = hit_match.end()
            same_line_value = orig[idx:].lstrip(" :=-—\t").strip()
            if same_line_value:
                value = same_line_value
            elif i + 1 < len(lines):
                value = lines[i + 1]
            else:
                continue

            value = value.rstrip(".,;:").strip()
            if value:
                result[key] = value
                break

    if result["business_email"] is None:
        em = _EMAIL_FALLBACK_RE.search(raw)
        if em:
            result["business_email"] = em.group(0).strip().rstrip(".,;:")

    if result["business_phone"] is None:
        for pm in _PHONE_FALLBACK_RE.finditer(raw):
            candidate = pm.group(0).strip()
            digits = re.sub(r"\D", "", candidate)
            if 8 <= len(digits) <= 15:
                result["business_phone"] = candidate
                break

    # Host-type classification
    joined_lc = " ".join(lowered)
    if "business host" in joined_lc or "professional" in joined_lc or "trader" in joined_lc:
        result["business_type"] = "Professional"
    elif "individual host" in joined_lc or "private host" in joined_lc or "persoana fizica" in joined_lc:
        result["business_type"] = "Individual"
    elif result["business_name"] or result["business_vat"] or result["business_registration_number"]:
        result["business_type"] = "Professional"
    else:
        result["business_type"] = "Unknown"

    return result


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

    # Price — prefer per-night rate from priceDetails
    pricing = item.get("pricingQuote", {})
    price = None
    currency = "USD"

    # Try explanationData.priceDetails for nightly rate
    pr = item.get("structuredDisplayPrice") or {}
    price_details = _get_nested(pr, "explanationData.priceDetails") or []
    for detail in price_details:
        if not isinstance(detail, dict):
            continue
        items = detail.get("items") or []
        for pd_item in items:
            desc = pd_item.get("description", "") if isinstance(pd_item, dict) else ""
            m = re.search(r"(\d+)\s*nights?\s*[x×]\s*[^\d]*(\d[\d.,]*)", desc)
            if m:
                price = float(m.group(2).replace(",", "."))
                break
        if price is not None:
            break

    # Fallback to pricingQuote.rate
    if price is None:
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
