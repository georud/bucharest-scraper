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

    rows = db.conn.execute("""
        SELECT platform, platform_id, name, latitude, longitude, property_type,
               star_rating, review_score, review_count, price_per_night, currency,
               url, thumbnail_url, bedrooms, beds, bathrooms, max_guests,
               is_superhost, grid_cell_id, scraped_at,
               business_name, business_registration_number, business_vat,
               business_address, business_email, business_phone,
               business_type, business_country, business_trade_register_name,
               host_name, host_id, host_response_rate, host_response_time, host_join_date
        FROM listings
        WHERE latitude != 0 AND longitude != 0
    """).fetchall()

    cols = [
        "platform", "platform_id", "name", "latitude", "longitude", "property_type",
        "star_rating", "review_score", "review_count", "price_per_night", "currency",
        "url", "thumbnail_url", "bedrooms", "beds", "bathrooms", "max_guests",
        "is_superhost", "grid_cell_id", "scraped_at",
        "business_name", "business_registration_number", "business_vat",
        "business_address", "business_email", "business_phone",
        "business_type", "business_country", "business_trade_register_name",
        "host_name", "host_id", "host_response_rate", "host_response_time", "host_join_date",
    ]

    booking_count = 0
    airbnb_count = 0

    for row in rows:
        r = dict(zip(cols, row))
        popup_html = _build_popup(r)

        color = BUSINESS_COLORS.get(r["business_type"] or "", {}).get(r["platform"]) \
            or PLATFORM_COLORS.get(r["platform"], "gray")

        marker = folium.Marker(
            location=[r["latitude"], r["longitude"]],
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
        '<div style="font-family:sans-serif;font-size:12px;line-height:1.4;max-width:400px;">',
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

    # Location
    loc_table = []
    loc_table.append(_row("Coords", f'{r["latitude"]:.5f}, {r["longitude"]:.5f}'))
    loc_table.append(_row("Cell", r["grid_cell_id"]))
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
