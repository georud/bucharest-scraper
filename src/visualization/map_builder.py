from __future__ import annotations

import html
import logging
from pathlib import Path

import folium
from folium.plugins import MarkerCluster

from ..config import DATA_DIR
from ..models.enums import Platform
from ..storage.database import Database

logger = logging.getLogger(__name__)

EXPORTS_DIR = DATA_DIR / "exports"

PLATFORM_COLORS = {
    Platform.BOOKING.value: "blue",
    Platform.AIRBNB.value: "red",
}

# Marker colour override for traders with full DSA disclosure — helps spot
# professional operators on the map at a glance.
BUSINESS_COLORS = {
    "Professional": {Platform.BOOKING.value: "darkblue", Platform.AIRBNB.value: "darkred"},
}

CENTER_LAT = 44.4268
CENTER_LNG = 26.1025


def build_map(db: Database, output_path: Path | None = None) -> Path:
    """Generate an interactive Folium map with all listings."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "bucharest_map.html"

    m = folium.Map(
        location=[CENTER_LAT, CENTER_LNG],
        zoom_start=12,
        tiles="cartodbpositron",
    )

    booking_cluster = MarkerCluster(name="Booking.com", show=True)
    airbnb_cluster = MarkerCluster(name="Airbnb", show=True)

    # Pull EVERY exported column (incl. the derived map_* position) via the same
    # shared definition the CSV/GeoJSON exporters use, so the map shows the full
    # record. Plotting uses map_latitude/map_longitude (= COALESCE(best, scraped)).
    from ..storage.exporter import _select_and_columns
    select_sql, cols = _select_and_columns()
    rows = db.conn.execute(
        f"SELECT {select_sql} FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "AND latitude != 0 AND longitude != 0"
    ).fetchall()

    booking_count = 0
    airbnb_count = 0

    for row in rows:
        r = dict(zip(cols, row))
        popup_html = _build_popup(r)

        color = BUSINESS_COLORS.get(r["business_type"] or "", {}).get(r["platform"]) \
            or PLATFORM_COLORS.get(r["platform"], "gray")

        marker = folium.Marker(
            location=[r["map_latitude"], r["map_longitude"]],
            popup=folium.Popup(popup_html, max_width=420),
            tooltip=f"{r['name']} · {r['platform']}",
            icon=folium.Icon(color=color, icon="info-sign"),
        )

        if r["platform"] == Platform.BOOKING.value:
            marker.add_to(booking_cluster)
            booking_count += 1
        else:
            marker.add_to(airbnb_cluster)
            airbnb_count += 1

    booking_cluster.add_to(m)
    airbnb_cluster.add_to(m)

    folium.LayerControl().add_to(m)

    legend_html = f"""
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
         background:white; padding:10px; border-radius:5px; border:1px solid #ccc;
         font-size:13px; font-family:sans-serif;">
        <b>Bucharest Listings</b><br>
        <span style="color:blue;">&#9679;</span> Booking ({booking_count:,})
        &nbsp; <span style="color:darkblue;">&#9679;</span> Booking trader<br>
        <span style="color:red;">&#9679;</span> Airbnb ({airbnb_count:,})
        &nbsp; <span style="color:darkred;">&#9679;</span> Airbnb trader<br>
        <b>Total: {booking_count + airbnb_count:,}</b>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(str(path))
    logger.info("Map saved to %s (%d Booking, %d Airbnb markers)", path, booking_count, airbnb_count)
    return path


def _esc(v) -> str:
    if v is None:
        return ""
    return html.escape(str(v))


def _row(label: str, value, raw_html: bool = False) -> str:
    if value is None or value == "":
        return ""
    v = value if raw_html else _esc(value)
    return (
        f'<tr><td style="color:#666;padding:1px 8px 1px 0;vertical-align:top;white-space:nowrap;">{_esc(label)}</td>'
        f'<td style="padding:1px 0;word-break:break-word;">{v}</td></tr>'
    )


def _build_popup(r: dict) -> str:
    """Build a rich HTML popup covering every captured field for this listing."""
    name = _esc(r["name"])
    platform = _esc(r["platform"]).title()
    platform_id = _esc(r["platform_id"])
    url = r["url"] or ""

    # Header
    parts = [
        '<div style="font-family:sans-serif;font-size:12px;line-height:1.4;max-width:400px;'
        'max-height:360px;overflow-y:auto;padding-right:4px;">',
        f'<div style="font-size:14px;font-weight:600;margin-bottom:2px;">{name}</div>',
        f'<div style="color:#888;margin-bottom:6px;">{platform}'
    ]
    if r["property_type"]:
        parts.append(f' · {_esc(r["property_type"])}')
    if r["is_superhost"]:
        parts.append(' · <span style="color:#d93">Superhost</span>')
    parts.append(f' · <span style="color:#aaa">#{platform_id}</span></div>')

    # Thumbnail
    thumb = r["thumbnail_url"]
    if thumb and str(thumb).startswith("http"):
        parts.append(
            f'<img src="{_esc(thumb)}" alt="" '
            'style="width:100%;max-height:140px;object-fit:cover;border-radius:4px;margin-bottom:6px;">'
        )

    # Price + rating block
    if r["price_per_night"] is not None:
        parts.append(
            f'<div style="font-size:15px;font-weight:600;margin-bottom:4px;">'
            f'{_esc(r["currency"] or "EUR")} {float(r["price_per_night"]):.0f}/night</div>'
        )

    review_bits = []
    if r["star_rating"]:
        stars_n = int(r["star_rating"])
        review_bits.append(f'{"★" * stars_n}{"☆" * (5 - stars_n)}')
    if r["review_score"] is not None:
        score_line = f'{float(r["review_score"]):.1f}/10'
        if r["review_count"]:
            score_line += f' · {int(r["review_count"]):,} reviews'
        review_bits.append(score_line)
    if review_bits:
        parts.append(f'<div style="color:#555;margin-bottom:6px;">{" &nbsp; ".join(review_bits)}</div>')

    # Room config
    room_bits = []
    for label, key in [("guests", "max_guests"), ("bed", "beds"), ("bedroom", "bedrooms"), ("bath", "bathrooms")]:
        v = r[key]
        if v is not None:
            try:
                vn = float(v)
                room_bits.append(f'{vn:g} {label}{"s" if vn != 1 else ""}')
            except (ValueError, TypeError):
                pass
    if room_bits:
        parts.append(f'<div style="color:#333;margin-bottom:8px;">{" · ".join(room_bits)}</div>')

    # Location, precision & dedup — every geo / identity field.
    def _coord(a, b):
        try:
            return f'{float(r[a]):.5f}, {float(r[b]):.5f}'
        except (TypeError, ValueError, KeyError):
            return None
    precision = r.get("location_precision") or "unverified"
    conf = r.get("position_confidence")
    source = r.get("location_source") or "platform_coord"
    accuracy = r.get("est_accuracy_m")
    radius = r.get("airbnb_location_radius_m")
    loc_table = [
        _row("Coords", _coord("map_latitude", "map_longitude")),
        _row("Location", precision + (f" (confidence {conf:.2f}, via {source})" if conf is not None else "")),
        _row("Accuracy", None if accuracy is None else f'{float(accuracy):.0f} m'),
        _row("Map source", r.get("map_source")),
        _row("Map precision", r.get("map_precision")),
        _row("Platform precision", r.get("platform_precision")),
        _row("Airbnb map radius", None if radius is None else f'{float(radius):.0f} m'),
        _row("Scraped coords", _coord("latitude", "longitude")),
        _row("Best coords", _coord("latitude_best", "longitude_best")),
        _row("Geocoded coords", _coord("latitude_geocoded", "longitude_geocoded")),
        _row("Geocoded address", r.get("geocoded_address")),
        _row("H3 cell", r.get("grid_cell_id")),
        _row("Operator id", r.get("operator_id")),
        _row("Property group", r.get("property_group_id")),
        _row("Cross-platform group", r.get("cross_platform_group_id")),
    ]
    loc_rows = "".join(x for x in loc_table if x)
    if loc_rows:
        parts.append(
            '<table style="border-collapse:collapse;font-size:11px;margin-bottom:6px;width:100%;">'
            + loc_rows + '</table>'
        )

    # Host section (name, id, responsiveness, tenure) — populated for Airbnb
    host_rows_raw = [
        _row("Host", r.get("host_name")),
        _row("Host id", r.get("host_id")),
        _row("Tenure", r.get("host_join_date")),
        _row("Response rate", r.get("host_response_rate")),
        _row("Response time", r.get("host_response_time")),
    ]
    host_rows = "".join(x for x in host_rows_raw if x)
    if host_rows:
        parts.append(
            '<div style="border-top:1px solid #eee;padding-top:6px;margin-top:4px;'
            'margin-bottom:6px;"><div style="font-weight:600;color:#444;margin-bottom:2px;">Host</div>'
            '<table style="border-collapse:collapse;font-size:11px;width:100%;">'
            + host_rows +
            '</table></div>'
        )

    # Business / DSA disclosure
    biz_rows_raw = [
        _row("Type", r["business_type"]),
        _row("Name", r["business_name"]),
        _row("Reg no.", r["business_registration_number"]),
        _row("Register", r["business_trade_register_name"]),
        _row("VAT", r["business_vat"]),
        _row("Address", r["business_address"]),
        _row("Country", r["business_country"]),
    ]
    # Email/phone as clickable links
    if r["business_email"]:
        biz_rows_raw.append(_row(
            "Email",
            f'<a href="mailto:{_esc(r["business_email"])}">{_esc(r["business_email"])}</a>',
            raw_html=True,
        ))
    if r["business_phone"]:
        tel = "".join(ch for ch in str(r["business_phone"]) if ch.isdigit() or ch == "+")
        biz_rows_raw.append(_row(
            "Phone",
            f'<a href="tel:{_esc(tel)}">{_esc(r["business_phone"])}</a>',
            raw_html=True,
        ))

    biz_rows = "".join(x for x in biz_rows_raw if x)
    if biz_rows:
        parts.append(
            '<div style="border-top:1px solid #eee;padding-top:6px;margin-top:4px;'
            'margin-bottom:6px;"><div style="font-weight:600;color:#444;margin-bottom:2px;">Business / trader</div>'
            '<table style="border-collapse:collapse;font-size:11px;width:100%;">'
            + biz_rows +
            '</table></div>'
        )

    # Provenance / extra (ids + first-seen + original price)
    prov_rows = "".join(x for x in [
        _row("Listing id", r.get("id")),
        _row("First seen", None if not r.get("first_seen_at") else str(r["first_seen_at"])[:16]),
        _row("Original price", None if r.get("price_original") is None
             else f'{_esc(r.get("currency_original") or "")} {float(r["price_original"]):.0f}'),
    ] if x)
    if prov_rows:
        parts.append(
            '<table style="border-collapse:collapse;font-size:11px;margin-bottom:6px;width:100%;">'
            + prov_rows + '</table>'
        )

    # Scraped-at + listing link
    footer = []
    if url and str(url).startswith("http"):
        footer.append(
            f'<a href="{_esc(url)}" target="_blank" '
            'style="font-weight:600;">Open listing ↗</a>'
        )
    if r["scraped_at"]:
        footer.append(f'<span style="color:#aaa;">scraped {_esc(str(r["scraped_at"])[:16])}</span>')
    if footer:
        parts.append(
            '<div style="border-top:1px solid #eee;padding-top:6px;margin-top:4px;'
            'display:flex;justify-content:space-between;font-size:11px;">'
            + '<div>' + footer[0] + '</div>'
            + ('<div>' + footer[1] + '</div>' if len(footer) > 1 else '')
            + '</div>'
        )

    parts.append('</div>')
    return "".join(parts)
