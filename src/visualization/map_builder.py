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

# Color scheme: blue=Booking, red=Airbnb
PLATFORM_COLORS = {
    Platform.BOOKING.value: "blue",
    Platform.AIRBNB.value: "red",
}

PLATFORM_ICONS = {
    Platform.BOOKING.value: "bed",
    Platform.AIRBNB.value: "home",
}

# Bucharest center
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

    # Create separate marker clusters per platform
    booking_cluster = MarkerCluster(name="Booking.com", show=True)
    airbnb_cluster = MarkerCluster(name="Airbnb", show=True)

    rows = db.conn.execute("""
        SELECT platform, name, latitude, longitude, property_type,
               star_rating, review_score, review_count, price_per_night,
               currency, url, is_superhost
        FROM listings
        WHERE latitude != 0 AND longitude != 0
    """).fetchall()

    booking_count = 0
    airbnb_count = 0

    for row in rows:
        (
            platform, name, lat, lng, prop_type,
            stars, score, reviews, price,
            currency, url, superhost,
        ) = row

        popup_html = _build_popup(
            name, platform, prop_type, stars, score, reviews, price, currency, url, superhost
        )

        color = PLATFORM_COLORS.get(platform, "gray")

        marker = folium.Marker(
            location=[lat, lng],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{name} ({platform})",
            icon=folium.Icon(color=color, icon="info-sign"),
        )

        if platform == Platform.BOOKING.value:
            marker.add_to(booking_cluster)
            booking_count += 1
        else:
            marker.add_to(airbnb_cluster)
            airbnb_count += 1

    booking_cluster.add_to(m)
    airbnb_cluster.add_to(m)

    folium.LayerControl().add_to(m)

    # Add legend
    legend_html = f"""
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
         background:white; padding:10px; border-radius:5px; border:1px solid #ccc;
         font-size:13px;">
        <b>Bucharest Listings</b><br>
        <span style="color:blue;">&#9679;</span> Booking.com ({booking_count:,})<br>
        <span style="color:red;">&#9679;</span> Airbnb ({airbnb_count:,})<br>
        <b>Total: {booking_count + airbnb_count:,}</b>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(str(path))
    logger.info("Map saved to %s (%d Booking, %d Airbnb markers)", path, booking_count, airbnb_count)
    return path


def _build_popup(
    name: str,
    platform: str,
    prop_type: str | None,
    stars: float | None,
    score: float | None,
    reviews: int | None,
    price: float | None,
    currency: str,
    url: str,
    superhost: bool | None,
) -> str:
    """Build HTML popup content for a map marker."""
    name = html.escape(name)
    platform = html.escape(platform)
    prop_type = html.escape(prop_type) if prop_type else prop_type
    lines = [f"<b>{name}</b><br>"]
    lines.append(f"<i>{platform.title()}</i>")

    if prop_type:
        lines.append(f" | {prop_type}")

    lines.append("<br>")

    if stars:
        lines.append(f"Stars: {'*' * int(stars)}<br>")

    if score is not None:
        lines.append(f"Score: {score:.1f}/10")
        if reviews:
            lines.append(f" ({reviews:,} reviews)")
        lines.append("<br>")

    if price is not None:
        lines.append(f"<b>{currency} {price:.0f}/night</b><br>")

    if superhost:
        lines.append("Superhost<br>")

    if url and url.startswith("https://"):
        lines.append(f'<a href="{html.escape(url)}" target="_blank">View listing</a>')

    return "".join(lines)
