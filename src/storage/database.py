from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from ..config import DATA_DIR
from ..models.enums import Platform, ScrapeStatus
from ..models.listing import Listing

logger = logging.getLogger(__name__)

DB_PATH = DATA_DIR / "bucharest.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_id TEXT NOT NULL,
    name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    property_type TEXT,
    star_rating REAL,
    review_score REAL,
    review_count INTEGER,
    price_per_night REAL,
    currency TEXT DEFAULT 'EUR',
    url TEXT,
    thumbnail_url TEXT,
    bedrooms INTEGER,
    beds INTEGER,
    bathrooms REAL,
    max_guests INTEGER,
    is_superhost INTEGER,
    scraped_at TEXT NOT NULL,
    grid_cell_id TEXT,
    raw_json TEXT,
    business_name TEXT,
    business_registration_number TEXT,
    business_vat TEXT,
    business_address TEXT,
    business_email TEXT,
    business_phone TEXT,
    business_type TEXT,
    business_country TEXT,
    business_trade_register_name TEXT,
    host_name TEXT,
    host_id TEXT,
    host_response_rate TEXT,
    host_response_time TEXT,
    host_join_date TEXT,
    price_original REAL,
    currency_original TEXT,
    cross_platform_group_id TEXT,
    first_seen_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_listings_platform ON listings(platform);
CREATE INDEX IF NOT EXISTS idx_listings_platform_id ON listings(platform, platform_id);
CREATE INDEX IF NOT EXISTS idx_listings_coords ON listings(latitude, longitude);

CREATE TABLE IF NOT EXISTS grid_progress (
    cell_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT,
    PRIMARY KEY (cell_id, platform)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    total_cells INTEGER,
    completed_cells INTEGER DEFAULT 0,
    total_listings INTEGER DEFAULT 0,
    listings_dropped INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'
);
"""


class Database:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Add columns that may be missing in older databases."""
        cursor = self.conn.execute("PRAGMA table_info(listings)")
        existing = {row[1] for row in cursor.fetchall()}
        new_columns = [
            ("bedrooms", "INTEGER"),
            ("beds", "INTEGER"),
            ("bathrooms", "REAL"),
            ("max_guests", "INTEGER"),
            ("business_name", "TEXT"),
            ("business_registration_number", "TEXT"),
            ("business_vat", "TEXT"),
            ("business_address", "TEXT"),
            ("business_email", "TEXT"),
            ("business_phone", "TEXT"),
            ("business_type", "TEXT"),
            ("business_country", "TEXT"),
            ("business_trade_register_name", "TEXT"),
            ("host_name", "TEXT"),
            ("host_id", "TEXT"),
            ("host_response_rate", "TEXT"),
            ("host_response_time", "TEXT"),
            ("host_join_date", "TEXT"),
            ("price_original", "REAL"),
            ("currency_original", "TEXT"),
            ("cross_platform_group_id", "TEXT"),
            ("first_seen_at", "TEXT"),
            ("operator_id", "TEXT"),
            ("property_group_id", "TEXT"),
            ("latitude_geocoded", "REAL"),
            ("longitude_geocoded", "REAL"),
            ("latitude_best", "REAL"),
            ("longitude_best", "REAL"),
            ("geocoded_address", "TEXT"),
            ("location_precision", "TEXT"),
            ("location_source", "TEXT"),
            ("est_accuracy_m", "REAL"),
            ("position_confidence", "REAL"),
        ]
        for col_name, col_type in new_columns:
            if col_name not in existing:
                self.conn.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
                logger.info("Migrated: added column %s to listings", col_name)

        # scrape_runs.listings_dropped — added after the table was first shipped
        run_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(scrape_runs)")}
        if "listings_dropped" not in run_cols:
            self.conn.execute("ALTER TABLE scrape_runs ADD COLUMN listings_dropped INTEGER DEFAULT 0")
            logger.info("Migrated: added column listings_dropped to scrape_runs")

        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS position_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id TEXT NOT NULL,
                property_group_id TEXT,
                capture_date TEXT,
                platform TEXT,
                source TEXT,            -- 'scraped' | 'geocoded'
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                sigma_m REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_obs_group ON position_observations(property_group_id);
            CREATE INDEX IF NOT EXISTS idx_obs_listing ON position_observations(listing_id);

            CREATE TABLE IF NOT EXISTS geocode_cache (
                address_norm TEXT PRIMARY KEY,
                status TEXT NOT NULL,          -- 'ok' | 'failed' | 'not_found'
                latitude REAL,
                longitude REAL,
                quality TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_tried_at TEXT
            );
        """)

        self.conn.commit()

    def upsert_listing(self, listing: Listing) -> bool:
        """Insert or update a listing. Returns True if new, False if updated."""
        sql = """
        INSERT INTO listings (
            id, platform, platform_id, name, latitude, longitude,
            property_type, star_rating, review_score, review_count,
            price_per_night, currency, url, thumbnail_url,
            bedrooms, beds, bathrooms, max_guests,
            is_superhost, scraped_at, grid_cell_id, raw_json,
            business_name, business_registration_number, business_vat,
            business_address, business_email, business_phone,
            business_type, business_country, business_trade_register_name,
            host_name, host_id, host_response_rate, host_response_time, host_join_date,
            price_original, currency_original, cross_platform_group_id, first_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=COALESCE(excluded.price_per_night, price_per_night),
            currency=excluded.currency,
            price_original=COALESCE(excluded.price_original, price_original),
            currency_original=COALESCE(excluded.currency_original, currency_original),
            review_score=COALESCE(excluded.review_score, review_score),
            review_count=COALESCE(excluded.review_count, review_count),
            bedrooms=COALESCE(excluded.bedrooms, bedrooms),
            beds=COALESCE(excluded.beds, beds),
            bathrooms=COALESCE(excluded.bathrooms, bathrooms),
            max_guests=COALESCE(excluded.max_guests, max_guests),
            business_name=COALESCE(excluded.business_name, business_name),
            business_registration_number=COALESCE(excluded.business_registration_number, business_registration_number),
            business_vat=COALESCE(excluded.business_vat, business_vat),
            business_address=COALESCE(excluded.business_address, business_address),
            business_email=COALESCE(excluded.business_email, business_email),
            business_phone=COALESCE(excluded.business_phone, business_phone),
            business_type=COALESCE(excluded.business_type, business_type),
            business_country=COALESCE(excluded.business_country, business_country),
            business_trade_register_name=COALESCE(excluded.business_trade_register_name, business_trade_register_name),
            host_name=COALESCE(excluded.host_name, host_name),
            host_id=COALESCE(excluded.host_id, host_id),
            host_response_rate=COALESCE(excluded.host_response_rate, host_response_rate),
            host_response_time=COALESCE(excluded.host_response_time, host_response_time),
            host_join_date=COALESCE(excluded.host_join_date, host_join_date),
            cross_platform_group_id=COALESCE(excluded.cross_platform_group_id, cross_platform_group_id),
            scraped_at=excluded.scraped_at,
            raw_json=excluded.raw_json
"""
        cursor = self.conn.execute(sql, listing.to_row())
        self.conn.commit()
        return cursor.rowcount > 0

    def upsert_listings(self, listings: list[Listing]) -> int:
        """Bulk upsert listings. Returns count of rows affected."""
        sql = """
        INSERT INTO listings (
            id, platform, platform_id, name, latitude, longitude,
            property_type, star_rating, review_score, review_count,
            price_per_night, currency, url, thumbnail_url,
            bedrooms, beds, bathrooms, max_guests,
            is_superhost, scraped_at, grid_cell_id, raw_json,
            business_name, business_registration_number, business_vat,
            business_address, business_email, business_phone,
            business_type, business_country, business_trade_register_name,
            host_name, host_id, host_response_rate, host_response_time, host_join_date,
            price_original, currency_original, cross_platform_group_id, first_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=COALESCE(excluded.price_per_night, price_per_night),
            currency=excluded.currency,
            price_original=COALESCE(excluded.price_original, price_original),
            currency_original=COALESCE(excluded.currency_original, currency_original),
            review_score=COALESCE(excluded.review_score, review_score),
            review_count=COALESCE(excluded.review_count, review_count),
            bedrooms=COALESCE(excluded.bedrooms, bedrooms),
            beds=COALESCE(excluded.beds, beds),
            bathrooms=COALESCE(excluded.bathrooms, bathrooms),
            max_guests=COALESCE(excluded.max_guests, max_guests),
            business_name=COALESCE(excluded.business_name, business_name),
            business_registration_number=COALESCE(excluded.business_registration_number, business_registration_number),
            business_vat=COALESCE(excluded.business_vat, business_vat),
            business_address=COALESCE(excluded.business_address, business_address),
            business_email=COALESCE(excluded.business_email, business_email),
            business_phone=COALESCE(excluded.business_phone, business_phone),
            business_type=COALESCE(excluded.business_type, business_type),
            business_country=COALESCE(excluded.business_country, business_country),
            business_trade_register_name=COALESCE(excluded.business_trade_register_name, business_trade_register_name),
            host_name=COALESCE(excluded.host_name, host_name),
            host_id=COALESCE(excluded.host_id, host_id),
            host_response_rate=COALESCE(excluded.host_response_rate, host_response_rate),
            host_response_time=COALESCE(excluded.host_response_time, host_response_time),
            host_join_date=COALESCE(excluded.host_join_date, host_join_date),
            cross_platform_group_id=COALESCE(excluded.cross_platform_group_id, cross_platform_group_id),
            scraped_at=excluded.scraped_at,
            raw_json=excluded.raw_json
"""
        rows = [l.to_row() for l in listings]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def enrich_listings(self, listings: list[Listing]) -> int:
        """NULL-safe upsert: only fill fields that are currently NULL."""
        sql = """
        UPDATE listings SET
            price_per_night = COALESCE(price_per_night, ?),
            currency = COALESCE(?, currency),
            price_original = COALESCE(price_original, ?),
            currency_original = COALESCE(currency_original, ?),
            bedrooms = COALESCE(bedrooms, ?),
            beds = COALESCE(beds, ?),
            bathrooms = COALESCE(bathrooms, ?),
            max_guests = COALESCE(max_guests, ?),
            is_superhost = COALESCE(is_superhost, ?),
            business_name = COALESCE(business_name, ?),
            business_registration_number = COALESCE(business_registration_number, ?),
            business_vat = COALESCE(business_vat, ?),
            business_address = COALESCE(business_address, ?),
            business_email = COALESCE(business_email, ?),
            business_phone = COALESCE(business_phone, ?),
            -- Allow a real classification (Professional/Individual/Private) to
            -- overwrite a placeholder 'Unknown'/NULL. NULLIF maps incoming
            -- 'Unknown' to NULL so it can't downgrade a real existing label;
            -- the trailing ? is the final fallback so 'Unknown' can still be
            -- written when both existing and the cleaned incoming are NULL.
            business_type = COALESCE(NULLIF(?, 'Unknown'), business_type, ?),
            business_country = COALESCE(business_country, ?),
            business_trade_register_name = COALESCE(business_trade_register_name, ?),
            host_name = COALESCE(host_name, ?),
            host_id = COALESCE(host_id, ?),
            host_response_rate = COALESCE(host_response_rate, ?),
            host_response_time = COALESCE(host_response_time, ?),
            host_join_date = COALESCE(host_join_date, ?),
            scraped_at = ?
        WHERE id = ?
        """
        rows = []
        for lst in listings:
            rows.append((
                lst.price_per_night,
                lst.currency,
                lst.price_original,
                lst.currency_original,
                lst.bedrooms,
                lst.beds,
                lst.bathrooms,
                lst.max_guests,
                lst.is_superhost,
                lst.business_name,
                lst.business_registration_number,
                lst.business_vat,
                lst.business_address,
                lst.business_email,
                lst.business_phone,
                lst.business_type,                 # for NULLIF(?, 'Unknown')
                lst.business_type,                 # fallback if both ends NULL
                lst.business_country,
                lst.business_trade_register_name,
                lst.host_name,
                lst.host_id,
                lst.host_response_rate,
                lst.host_response_time,
                lst.host_join_date,
                lst.scraped_at.isoformat(),
                lst.id,
            ))
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def get_listings_missing_data(self, platform: Platform) -> list[Listing]:
        """Get listings that are missing room data (beds, bathrooms, max_guests all NULL)."""
        rows = self.conn.execute(
            """SELECT id, platform, platform_id, name, latitude, longitude,
                      property_type, star_rating, review_score, review_count,
                      price_per_night, currency, url, thumbnail_url,
                      bedrooms, beds, bathrooms, max_guests,
                      is_superhost, scraped_at, grid_cell_id, raw_json,
                      business_name, business_registration_number, business_vat,
                      business_address, business_email, business_phone,
                      business_type, business_country, business_trade_register_name,
                      host_name, host_id, host_response_rate, host_response_time, host_join_date,
                      price_original, currency_original, cross_platform_group_id, first_seen_at
               FROM listings
               WHERE platform = ?
                 AND beds IS NULL AND bathrooms IS NULL AND max_guests IS NULL""",
            (platform.value,),
        ).fetchall()

        listings = []
        for row in rows:
            listings.append(Listing(
                id=row[0], platform=Platform(row[1]), platform_id=row[2],
                name=row[3], latitude=row[4], longitude=row[5],
                property_type=row[6], star_rating=row[7],
                review_score=row[8], review_count=row[9],
                price_per_night=row[10], currency=row[11],
                url=row[12], thumbnail_url=row[13],
                bedrooms=row[14], beds=row[15], bathrooms=row[16],
                max_guests=row[17], is_superhost=bool(row[18]) if row[18] is not None else None,
                scraped_at=datetime.fromisoformat(row[19]) if row[19] else datetime.utcnow(),
                grid_cell_id=row[20] or "", raw_json=row[21],
                business_name=row[22], business_registration_number=row[23],
                business_vat=row[24], business_address=row[25],
                business_email=row[26], business_phone=row[27],
                business_type=row[28], business_country=row[29],
                business_trade_register_name=row[30],
                host_name=row[31], host_id=row[32],
                host_response_rate=row[33], host_response_time=row[34],
                host_join_date=row[35],
                price_original=row[36], currency_original=row[37],
                cross_platform_group_id=row[38],
                first_seen_at=datetime.fromisoformat(row[39]) if row[39] else datetime.utcnow(),
            ))
        return listings

    def get_listings_missing_prices(self, platform: Platform) -> list[Listing]:
        """Get listings that have no price_per_night."""
        rows = self.conn.execute(
            """SELECT id, platform, platform_id, name, latitude, longitude,
                      property_type, star_rating, review_score, review_count,
                      price_per_night, currency, url, thumbnail_url,
                      bedrooms, beds, bathrooms, max_guests,
                      is_superhost, scraped_at, grid_cell_id, raw_json,
                      business_name, business_registration_number, business_vat,
                      business_address, business_email, business_phone,
                      business_type, business_country, business_trade_register_name,
                      host_name, host_id, host_response_rate, host_response_time, host_join_date,
                      price_original, currency_original, cross_platform_group_id, first_seen_at
               FROM listings
               WHERE platform = ? AND price_per_night IS NULL""",
            (platform.value,),
        ).fetchall()

        listings = []
        for row in rows:
            listings.append(Listing(
                id=row[0], platform=Platform(row[1]), platform_id=row[2],
                name=row[3], latitude=row[4], longitude=row[5],
                property_type=row[6], star_rating=row[7],
                review_score=row[8], review_count=row[9],
                price_per_night=row[10], currency=row[11],
                url=row[12], thumbnail_url=row[13],
                bedrooms=row[14], beds=row[15], bathrooms=row[16],
                max_guests=row[17], is_superhost=bool(row[18]) if row[18] is not None else None,
                scraped_at=datetime.fromisoformat(row[19]) if row[19] else datetime.utcnow(),
                grid_cell_id=row[20] or "", raw_json=row[21],
                business_name=row[22], business_registration_number=row[23],
                business_vat=row[24], business_address=row[25],
                business_email=row[26], business_phone=row[27],
                business_type=row[28], business_country=row[29],
                business_trade_register_name=row[30],
                host_name=row[31], host_id=row[32],
                host_response_rate=row[33], host_response_time=row[34],
                host_join_date=row[35],
                price_original=row[36], currency_original=row[37],
                cross_platform_group_id=row[38],
                first_seen_at=datetime.fromisoformat(row[39]) if row[39] else datetime.utcnow(),
            ))
        return listings

    def get_listing_count(self, platform: Platform | None = None) -> int:
        if platform:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM listings WHERE platform=?",
                (platform.value,),
            ).fetchone()
        else:
            row = self.conn.execute("SELECT COUNT(*) FROM listings").fetchone()
        return row[0]

    def count_missing_prices(self, platform: Platform) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM listings WHERE platform = ? AND price_per_night IS NULL",
            (platform.value,),
        ).fetchone()
        return row[0]

    def count_missing_business_data(self, platform: Platform) -> int:
        row = self.conn.execute(
            """SELECT COUNT(*) FROM listings
               WHERE platform = ?
                 AND (
                   business_type IS NULL
                   OR business_type = 'Unknown'
                   OR (platform = 'booking' AND business_type = 'Professional'
                       AND business_trade_register_name IS NULL)
                 )""",
            (platform.value,),
        ).fetchone()
        return row[0]

    def get_listings_missing_business_data(self, platform: Platform, limit: int | None = None) -> list[Listing]:
        """Return listings that need a detail-page fetch for business disclosure.

        Includes:
          - Never-checked listings (business_type IS NULL)
          - Professional listings still missing `business_trade_register_name` — i.e.
            checked under an earlier parser version. Re-fetch is safe and idempotent.
        """
        # Note: `business_trade_register_name` is Booking-only (Airbnb doesn't
        # expose it), so that sub-clause is scoped with `platform = 'booking'`.
        sql = """SELECT id, platform, platform_id, name, latitude, longitude,
                        property_type, star_rating, review_score, review_count,
                        price_per_night, currency, url, thumbnail_url,
                        bedrooms, beds, bathrooms, max_guests,
                        is_superhost, scraped_at, grid_cell_id, raw_json,
                        business_name, business_registration_number, business_vat,
                        business_address, business_email, business_phone,
                        business_type, business_country, business_trade_register_name,
                        host_name, host_id, host_response_rate, host_response_time, host_join_date,
                        price_original, currency_original, cross_platform_group_id, first_seen_at
                 FROM listings
                 WHERE platform = ?
                   AND (
                     business_type IS NULL
                     OR business_type = 'Unknown'
                     OR (platform = 'booking' AND business_type = 'Professional'
                         AND business_trade_register_name IS NULL)
                   )"""
        params: tuple = (platform.value,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (platform.value, limit)
        rows = self.conn.execute(sql, params).fetchall()

        listings = []
        for row in rows:
            listings.append(Listing(
                id=row[0], platform=Platform(row[1]), platform_id=row[2],
                name=row[3], latitude=row[4], longitude=row[5],
                property_type=row[6], star_rating=row[7],
                review_score=row[8], review_count=row[9],
                price_per_night=row[10], currency=row[11],
                url=row[12], thumbnail_url=row[13],
                bedrooms=row[14], beds=row[15], bathrooms=row[16],
                max_guests=row[17], is_superhost=bool(row[18]) if row[18] is not None else None,
                scraped_at=datetime.fromisoformat(row[19]) if row[19] else datetime.utcnow(),
                grid_cell_id=row[20] or "", raw_json=row[21],
                business_name=row[22], business_registration_number=row[23],
                business_vat=row[24], business_address=row[25],
                business_email=row[26], business_phone=row[27],
                business_type=row[28], business_country=row[29],
                business_trade_register_name=row[30],
                host_name=row[31], host_id=row[32],
                host_response_rate=row[33], host_response_time=row[34],
                host_join_date=row[35],
                price_original=row[36], currency_original=row[37],
                cross_platform_group_id=row[38],
                first_seen_at=datetime.fromisoformat(row[39]) if row[39] else datetime.utcnow(),
            ))
        return listings

    def get_distinct_cell_ids(self) -> list[str]:
        """Return all distinct grid_cell_id values that have listings."""
        rows = self.conn.execute(
            "SELECT DISTINCT grid_cell_id FROM listings WHERE grid_cell_id IS NOT NULL AND grid_cell_id != ''"
        ).fetchall()
        return [r[0] for r in rows]

    # -- Grid progress --

    def init_grid_progress(self, cell_ids: list[str], platforms: list[Platform]):
        """Initialize progress tracking for all grid cells."""
        sql = """
        INSERT OR IGNORE INTO grid_progress (cell_id, platform, status)
        VALUES (?, ?, 'pending')
        """
        rows = [
            (cid, p.value) for cid in cell_ids for p in platforms
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()

    def update_cell_status(
        self,
        cell_id: str,
        platform: Platform,
        status: ScrapeStatus,
        result_count: int = 0,
        error_message: str | None = None,
    ):
        now = datetime.utcnow().isoformat()
        if status == ScrapeStatus.IN_PROGRESS:
            self.conn.execute(
                "UPDATE grid_progress SET status=?, started_at=? WHERE cell_id=? AND platform=?",
                (status.value, now, cell_id, platform.value),
            )
        elif status in (ScrapeStatus.COMPLETED, ScrapeStatus.FAILED, ScrapeStatus.NEEDS_REFINEMENT):
            self.conn.execute(
                "UPDATE grid_progress SET status=?, result_count=?, completed_at=?, error_message=? "
                "WHERE cell_id=? AND platform=?",
                (status.value, result_count, now, error_message, cell_id, platform.value),
            )
        self.conn.commit()

    def get_pending_cells(self, platform: Platform) -> list[str]:
        rows = self.conn.execute(
            "SELECT cell_id FROM grid_progress WHERE platform=? AND status='pending'",
            (platform.value,),
        ).fetchall()
        return [r[0] for r in rows]

    def cell_has_listings(self, cell_id: str, platform: Platform) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM listings WHERE grid_cell_id = ? AND platform = ? LIMIT 1",
            (cell_id, platform.value),
        ).fetchone()
        return row is not None

    def get_cells_needing_refinement(self, platform: Platform) -> list[str]:
        rows = self.conn.execute(
            "SELECT cell_id FROM grid_progress WHERE platform=? AND status='needs_refinement'",
            (platform.value,),
        ).fetchall()
        return [r[0] for r in rows]

    def get_progress_summary(self) -> dict:
        rows = self.conn.execute("""
            SELECT platform, status, COUNT(*), SUM(result_count)
            FROM grid_progress
            GROUP BY platform, status
        """).fetchall()

        summary = {}
        for platform, status, count, total in rows:
            if platform not in summary:
                summary[platform] = {}
            summary[platform][status] = {"cells": count, "results": total or 0}
        return summary

    # -- Scrape runs --

    def start_run(self, total_cells: int) -> int:
        cursor = self.conn.execute(
            "INSERT INTO scrape_runs (started_at, total_cells) VALUES (?, ?)",
            (datetime.utcnow().isoformat(), total_cells),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_run(self, run_id: int, completed_cells: int, total_listings: int):
        self.conn.execute(
            "UPDATE scrape_runs SET completed_cells=?, total_listings=? WHERE id=?",
            (completed_cells, total_listings, run_id),
        )
        self.conn.commit()

    def finish_run(
        self,
        run_id: int,
        total_listings: int | None = None,
        completed_cells: int | None = None,
        listings_dropped: int | None = None,
    ):
        """Mark a run complete and record final tallies for the audit trail."""
        self.conn.execute(
            """UPDATE scrape_runs
               SET completed_at = ?,
                   status = 'completed',
                   total_listings = COALESCE(?, total_listings),
                   completed_cells = COALESCE(?, completed_cells),
                   listings_dropped = COALESCE(?, listings_dropped)
               WHERE id = ?""",
            (
                datetime.utcnow().isoformat(),
                total_listings,
                completed_cells,
                listings_dropped,
                run_id,
            ),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Cross-platform linking + operator aggregation (data-journalism views)
    # ------------------------------------------------------------------

    def set_cross_platform_groups(self, mapping: dict[str, str]) -> int:
        """Bulk-write `cross_platform_group_id` for matched listings.

        `mapping` is {listing_id: group_id}. Listings not in the mapping are
        left untouched. Returns the number of rows updated.
        """
        if not mapping:
            return 0
        rows = [(group_id, listing_id) for listing_id, group_id in mapping.items()]
        self.conn.executemany(
            "UPDATE listings SET cross_platform_group_id = ? WHERE id = ?",
            rows,
        )
        self.conn.commit()
        return len(rows)

    @staticmethod
    def read_historical_observations(db_path, platform_sigma=None) -> list[tuple]:
        """Read (listing_id, lat, lng, sigma_m, capture_date, platform) from a
        prior DB file for temporal fusion. sigma defaults: booking 60, airbnb 100."""
        import sqlite3 as _sq
        sigma = platform_sigma or {"booking": 60.0, "airbnb": 100.0}
        conn = _sq.connect(str(db_path))
        try:
            rows = conn.execute(
                "SELECT id, latitude, longitude, platform, scraped_at FROM listings "
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()
        return [(r[0], r[1], r[2], sigma.get(r[3], 100.0), (r[4] or "")[:10], r[3]) for r in rows]

    def get_all_listings_minimal(self) -> list[Listing]:
        """Return every listing with the fields the deduplicator needs.

        Lighter than the `get_listings_missing_*` readers — used by the
        cross-platform linking phase, which only needs id/platform/name/coords.
        """
        rows = self.conn.execute(
            "SELECT id, platform, platform_id, name, latitude, longitude "
            "FROM listings WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
        return [
            Listing(
                id=r[0], platform=Platform(r[1]), platform_id=r[2],
                name=r[3] or "", latitude=r[4], longitude=r[5],
            )
            for r in rows
        ]

    def get_operator_summary(self) -> list[dict]:
        """Group listings by operator for the 'who controls what' view.

        Operator key precedence: business_registration_number → business_name →
        host_id. Only rows with at least one of those are returned. One row per
        operator, with listing count, platforms present and professional flag.
        """
        rows = self.conn.execute(
            """
            SELECT
                COALESCE(business_registration_number, business_name, host_id) AS operator_key,
                COALESCE(MAX(business_name), MAX(host_name))                   AS operator_name,
                MAX(business_registration_number)                             AS registration_number,
                MAX(business_trade_register_name)                             AS trade_register,
                GROUP_CONCAT(DISTINCT platform)                               AS platforms,
                COUNT(*)                                                      AS listing_count,
                SUM(CASE WHEN business_type = 'Professional' THEN 1 ELSE 0 END) AS professional_listings
            FROM listings
            WHERE COALESCE(business_registration_number, business_name, host_id) IS NOT NULL
            GROUP BY operator_key
            ORDER BY listing_count DESC
            """
        ).fetchall()
        return [
            {
                "operator_key": r[0],
                "operator_name": r[1],
                "registration_number": r[2],
                "trade_register": r[3],
                "platforms": r[4],
                "listing_count": r[5],
                "professional_listings": r[6],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Geo / dedup curation stage
    # ------------------------------------------------------------------

    _CURATION_COLS = (
        "id", "platform", "name", "latitude", "longitude",
        "bedrooms", "beds", "bathrooms", "business_type",
        "business_registration_number", "business_phone", "business_email",
        "host_name", "host_id", "raw_json", "scraped_at",
    )

    def get_listings_for_curation(self) -> list[dict]:
        """Lightweight dict reader for the curation stage (identity + room +
        coords + raw_json). Only rows with valid coordinates."""
        cols = ", ".join(self._CURATION_COLS)
        rows = self.conn.execute(
            f"SELECT {cols} FROM listings "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
        return [dict(zip(self._CURATION_COLS, r)) for r in rows]

    def set_operator_ids(self, mapping: dict[str, str]) -> int:
        if not mapping:
            return 0
        self.conn.executemany(
            "UPDATE listings SET operator_id=? WHERE id=?",
            [(op, lid) for lid, op in mapping.items()],
        )
        self.conn.commit()
        return len(mapping)

    def reset_curation_columns(self) -> None:
        """Clear all curation-derived columns so the stage recomputes from
        scratch (curation is authoritative for these). The geocode_cache table
        is NOT touched, so re-geocoding stays cache-fast."""
        self.conn.execute(
            """UPDATE listings SET
                 operator_id=NULL, property_group_id=NULL, cross_platform_group_id=NULL,
                 latitude_geocoded=NULL, longitude_geocoded=NULL,
                 latitude_best=NULL, longitude_best=NULL, geocoded_address=NULL,
                 location_precision=NULL, location_source=NULL,
                 est_accuracy_m=NULL, position_confidence=NULL"""
        )
        self.conn.commit()

    def set_property_groups(self, mapping: dict[str, str],
                            cross_platform: set[str]) -> int:
        """Write property_group_id; set cross_platform_group_id to the group id
        only for groups in `cross_platform` (those spanning both platforms).
        The curation stage is authoritative for property_group_id /
        cross_platform_group_id and resets these columns before calling this;
        listings not in `mapping` are left untouched."""
        if not mapping:
            return 0
        rows = []
        for lid, gid in mapping.items():
            rows.append((gid, gid if gid in cross_platform else None, lid))
        self.conn.executemany(
            "UPDATE listings SET property_group_id=?, cross_platform_group_id=? WHERE id=?",
            rows,
        )
        self.conn.commit()
        return len(mapping)

    def replace_position_observations(self, observations: list[tuple]) -> int:
        """Atomically clear the ledger and insert new observations in one transaction.

        Each tuple is (listing_id, property_group_id, capture_date, platform, source,
        latitude, longitude, sigma_m). Returns the number of rows inserted.
        If `observations` is empty, still clears the table and returns 0.
        """
        now = datetime.utcnow().isoformat()
        self.conn.execute("DELETE FROM position_observations")
        if observations:
            self.conn.executemany(
                """INSERT INTO position_observations
                   (listing_id, property_group_id, capture_date, platform, source,
                    latitude, longitude, sigma_m, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [obs + (now,) for obs in observations],
            )
        self.conn.commit()
        return len(observations)

    def add_position_observations(self, observations: list[tuple]) -> int:
        """observations: list of (listing_id, property_group_id, capture_date,
        platform, source, latitude, longitude, sigma_m)."""
        if not observations:
            return 0
        now = datetime.utcnow().isoformat()
        self.conn.executemany(
            """INSERT INTO position_observations
               (listing_id, property_group_id, capture_date, platform, source,
                latitude, longitude, sigma_m, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [obs + (now,) for obs in observations],
        )
        self.conn.commit()
        return len(observations)

    def set_geocoded(self, mapping: dict[str, tuple]) -> int:
        """mapping: {listing_id: (lat, lng, geocoded_address)}."""
        if not mapping:
            return 0
        self.conn.executemany(
            "UPDATE listings SET latitude_geocoded=?, longitude_geocoded=?, geocoded_address=? WHERE id=?",
            [(v[0], v[1], v[2], lid) for lid, v in mapping.items()],
        )
        self.conn.commit()
        return len(mapping)

    def set_fused_positions(self, mapping: dict[str, dict]) -> int:
        """mapping: {listing_id: {lat_best, lng_best, est_accuracy_m,
        position_confidence, location_source, location_precision}}."""
        if not mapping:
            return 0
        rows = [
            (v["lat_best"], v["lng_best"], v["est_accuracy_m"],
             v["position_confidence"], v["location_source"], v["location_precision"], lid)
            for lid, v in mapping.items()
        ]
        self.conn.executemany(
            """UPDATE listings SET latitude_best=?, longitude_best=?, est_accuracy_m=?,
               position_confidence=?, location_source=?, location_precision=? WHERE id=?""",
            rows,
        )
        self.conn.commit()
        return len(mapping)

    def get_geocode(self, address_norm: str) -> dict | None:
        row = self.conn.execute(
            "SELECT address_norm, status, latitude, longitude, quality, attempts, last_tried_at "
            "FROM geocode_cache WHERE address_norm=?", (address_norm,)
        ).fetchone()
        if not row:
            return None
        return dict(zip(
            ("address_norm", "status", "latitude", "longitude", "quality", "attempts", "last_tried_at"), row))

    def upsert_geocode(self, address_norm: str, status: str, latitude: float | None,
                       longitude: float | None, quality: str | None, attempts: int) -> None:
        self.conn.execute(
            """INSERT INTO geocode_cache
               (address_norm, status, latitude, longitude, quality, attempts, last_tried_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(address_norm) DO UPDATE SET
                 status=excluded.status, latitude=excluded.latitude,
                 longitude=excluded.longitude, quality=excluded.quality,
                 attempts=excluded.attempts, last_tried_at=excluded.last_tried_at""",
            (address_norm, status, latitude, longitude, quality, attempts,
             datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
