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
    host_join_date TEXT
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
        ]
        for col_name, col_type in new_columns:
            if col_name not in existing:
                self.conn.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
                logger.info("Migrated: added column %s to listings", col_name)
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
            host_name, host_id, host_response_rate, host_response_time, host_join_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=COALESCE(excluded.price_per_night, price_per_night),
            currency=excluded.currency,
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
            host_name, host_id, host_response_rate, host_response_time, host_join_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=COALESCE(excluded.price_per_night, price_per_night),
            currency=excluded.currency,
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
            business_type = COALESCE(business_type, ?),
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
                lst.business_type,
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
                      host_name, host_id, host_response_rate, host_response_time, host_join_date
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
                      host_name, host_id, host_response_rate, host_response_time, host_join_date
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
                        host_name, host_id, host_response_rate, host_response_time, host_join_date
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

    def finish_run(self, run_id: int):
        self.conn.execute(
            "UPDATE scrape_runs SET completed_at=?, status='completed' WHERE id=?",
            (datetime.utcnow().isoformat(), run_id),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
