# Location Precision, Identity Dedup & GPS Fusion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a re-runnable post-enrichment curation stage that tags each listing's location precision, deduplicates listings using identity keys (operator + property level), and fuses cross-platform + temporal coordinates into a calculated best position — preserving every original coordinate.

**Architecture:** A new `src/geo/` package (precision, geocoding, fusion, curation orchestration) plus extensions to `src/dedup/` (operators, layered property groups, validation). The `Listing` dataclass and scraper/enrichment paths are **untouched** — new columns are added by the idempotent migration, written by dedicated `Database` `UPDATE` methods, and read by exporters via direct SQL. The stage runs via `python -m src.orchestrator --curate-only` on the existing DB, then inline during a full rescrape.

**Tech Stack:** Python 3.11, sqlite3, rapidfuzz (already a dep), stdlib `urllib` for Nominatim (no new HTTP dep), pytest (added here — no suite exists yet).

**Spec:** `docs/superpowers/specs/2026-05-21-location-precision-dedup-gps-fusion-design.md`

---

## File Structure

**New files:**
- `tests/__init__.py`, `tests/conftest.py` — pytest scaffold + fixtures
- `src/geo/__init__.py`
- `src/geo/precision.py` — scraped-coordinate precision classification + Booking address extraction
- `src/geo/fusion.py` — local projection, inverse-variance fusion, confidence
- `src/geo/geocode.py` — Nominatim client (urllib, rate-limited) + DB-backed cache with persistent retry
- `src/geo/curate.py` — `run_curation()` orchestrating the whole stage
- `src/dedup/operators.py` — identity normalization + operator union-find
- `src/dedup/property_groups.py` — layered (Tier 0/1/2) property dedup
- `src/dedup/validate.py` — identity-key ground-truth metrics
- `tests/test_*.py` — one per module above

**Modified files:**
- `src/storage/database.py` — migration (new columns + `position_observations`, `geocode_cache` tables); curation readers/writers; re-key `get_operator_summary`
- `src/storage/exporter.py` — new columns, GeoJSON uses best coords, review/metrics exports
- `src/config.py` + `config/scraping.yaml` — `geocoding` / `dedup` / `fusion` config
- `src/orchestrator.py` — replace `_link_cross_platform` with the curation stage; `--curate-only` CLI
- `src/visualization/map_builder.py` — plot best coords, show precision/confidence
- `pyproject.toml` — pytest dev dependency

---

## Task 0: Test scaffold + dev dependency

**Files:**
- Modify: `pyproject.toml`
- Create: `tests/__init__.py`, `tests/conftest.py`, `tests/test_smoke.py`

- [ ] **Step 1: Add a pytest dev-dependency group to `pyproject.toml`**

Add after the `[project.scripts]` block:

```toml
[project.optional-dependencies]
dev = ["pytest>=8.0"]
```

- [ ] **Step 2: Install it**

Run: `python -m pip install -e ".[dev]"`
Expected: installs pytest successfully.

- [ ] **Step 3: Create the test package + a shared in-memory DB fixture**

`tests/__init__.py`: empty file.

`tests/conftest.py`:

```python
import pytest

from src.storage.database import Database


@pytest.fixture
def db(tmp_path):
    """A fresh Database backed by a temp file (schema + migrations applied)."""
    database = Database(db_path=tmp_path / "test.db")
    yield database
    database.close()
```

- [ ] **Step 4: Write a smoke test**

`tests/test_smoke.py`:

```python
def test_db_fixture_has_listings_table(db):
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(listings)")}
    assert "latitude" in cols
    assert "cross_platform_group_id" in cols
```

- [ ] **Step 5: Run it**

Run: `python -m pytest tests/ -v`
Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml tests/
git commit -m "test: bootstrap pytest scaffold with shared db fixture"
```

---

## Task 1: Identity normalization

**Files:**
- Create: `src/dedup/operators.py`
- Test: `tests/test_operators.py`

- [ ] **Step 1: Write failing tests**

`tests/test_operators.py`:

```python
from src.dedup.operators import (
    normalize_registration, normalize_phone, normalize_email,
)


def test_normalize_registration_strips_ro_and_punctuation():
    assert normalize_registration("RO 41137103") == "41137103"
    assert normalize_registration("J40/1234/2020") == "j4012342020"
    assert normalize_registration(None) is None
    assert normalize_registration("   ") is None


def test_normalize_phone_keeps_last_9_digits():
    assert normalize_phone("+40 721 234 567") == "721234567"
    assert normalize_phone("0040721234567") == "721234567"
    assert normalize_phone("0721234567") == "721234567"
    assert normalize_phone("123") is None  # too short to be an identity key


def test_normalize_email_lowercases_and_trims():
    assert normalize_email("  Host@Example.COM ") == "host@example.com"
    assert normalize_email("not-an-email") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_operators.py -v`
Expected: FAIL (module `src.dedup.operators` not found).

- [ ] **Step 3: Implement**

`src/dedup/operators.py`:

```python
from __future__ import annotations

import hashlib
import re


def normalize_registration(value: str | None) -> str | None:
    """Lowercase, strip to [a-z0-9], drop a leading 'ro' VAT prefix."""
    if not value:
        return None
    cleaned = re.sub(r"[^a-z0-9]", "", value.lower())
    if cleaned.startswith("ro"):
        cleaned = cleaned[2:]
    return cleaned or None


def normalize_phone(value: str | None) -> str | None:
    """Digits only; drop RO trunk/country prefixes; keep the 9-digit national
    number. Returns None if fewer than 9 digits (too weak to be an identity key)."""
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    for prefix in ("0040", "40", "0"):
        if digits.startswith(prefix) and len(digits) - len(prefix) >= 9:
            digits = digits[len(prefix):]
            break
    if len(digits) < 9:
        return None
    return digits[-9:]


def normalize_email(value: str | None) -> str | None:
    """Lowercase + trim; return None if it isn't a plausible email."""
    if not value:
        return None
    cleaned = value.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", cleaned):
        return None
    return cleaned
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_operators.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dedup/operators.py tests/test_operators.py
git commit -m "feat: identity-key normalization for operator linking"
```

---

## Task 2: Operator union-find

**Files:**
- Modify: `src/dedup/operators.py`
- Test: `tests/test_operators.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_operators.py`:

```python
from src.dedup.operators import assign_operator_ids


class _Row(dict):
    """dict with attribute access, mimicking a curation row."""
    __getattr__ = dict.get


def _row(id, reg=None, phone=None, email=None):
    return _Row(id=id, business_registration_number=reg,
                business_phone=phone, business_email=email)


def test_operator_union_find_links_by_shared_phone():
    rows = [
        _row("booking_1", phone="+40 721 000 111"),
        _row("airbnb_2", phone="0721000111"),     # same phone -> same operator
        _row("airbnb_3", reg="RO 999"),           # different operator
    ]
    mapping = assign_operator_ids(rows)
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping["airbnb_3"] != mapping["booking_1"]


def test_operator_id_is_stable_and_listings_without_keys_excluded():
    rows = [_row("booking_1", reg="RO123"), _row("airbnb_9")]
    mapping = assign_operator_ids(rows)
    assert "airbnb_9" not in mapping            # no identity key -> no operator
    assert mapping["booking_1"].startswith("op_")
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_operators.py -v`
Expected: FAIL (`assign_operator_ids` not defined).

- [ ] **Step 3: Implement**

Append to `src/dedup/operators.py`:

```python
def assign_operator_ids(rows) -> dict[str, str]:
    """Union-find over listings sharing any normalized identity key
    (registration / phone / email). Returns {listing_id: operator_id} for
    listings that carry at least one identity key. operator_id is a stable hash
    of the operator's sorted identity keys.

    Safe to use union-find here: a shared registration/phone/email genuinely
    identifies one operator (unlike GPS+name, which can chain distinct flats).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # key -> first listing id seen with that key; union subsequent sharers.
    key_owner: dict[str, str] = {}
    has_key: set[str] = set()
    for row in rows:
        keys = [
            normalize_registration(row.get("business_registration_number")),
            normalize_phone(row.get("business_phone")),
            normalize_email(row.get("business_email")),
        ]
        for key in keys:
            if not key:
                continue
            has_key.add(row["id"])
            find(row["id"])
            if key in key_owner:
                union(key_owner[key], row["id"])
            else:
                key_owner[key] = row["id"]

    # Collect the identity keys per component to build a stable operator_id.
    comp_keys: dict[str, set[str]] = {}
    for row in rows:
        if row["id"] not in has_key:
            continue
        root = find(row["id"])
        bucket = comp_keys.setdefault(root, set())
        for key in (
            normalize_registration(row.get("business_registration_number")),
            normalize_phone(row.get("business_phone")),
            normalize_email(row.get("business_email")),
        ):
            if key:
                bucket.add(key)

    root_to_opid = {
        root: "op_" + hashlib.md5("|".join(sorted(keys)).encode()).hexdigest()[:16]
        for root, keys in comp_keys.items()
    }
    return {lid: root_to_opid[find(lid)] for lid in has_key}
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_operators.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dedup/operators.py tests/test_operators.py
git commit -m "feat: operator union-find over shared identity keys"
```

---

## Task 3: Schema migration + curation reader/writers in Database

**Files:**
- Modify: `src/storage/database.py:103-142` (`_migrate`), and append new methods before `close()`
- Test: `tests/test_database_curation.py`

- [ ] **Step 1: Write failing test**

`tests/test_database_curation.py`:

```python
from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing


def _mk(db, lid, platform, name, lat, lng, **kw):
    db.upsert_listing(Listing(
        id=lid, platform=platform, platform_id=lid.split("_")[1],
        name=name, latitude=lat, longitude=lng,
        scraped_at=datetime(2026, 5, 15), first_seen_at=datetime(2026, 5, 15), **kw,
    ))


def test_new_columns_present(db):
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(listings)")}
    for c in ("operator_id", "property_group_id", "latitude_geocoded",
              "latitude_best", "location_precision", "location_source",
              "est_accuracy_m", "position_confidence", "geocoded_address"):
        assert c in cols


def test_new_tables_present(db):
    names = {r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "position_observations" in names
    assert "geocode_cache" in names


def test_get_listings_for_curation_and_setters(db):
    _mk(db, "booking_1", Platform.BOOKING, "Old Town Flat", 44.43, 26.10,
        business_phone="0721000111", raw_json='{"location":{"address":"Strada X 5","city":"Bucuresti"}}')
    _mk(db, "airbnb_2", Platform.AIRBNB, "Old Town Flat", 44.43, 26.10,
        business_phone="+40721000111")

    rows = db.get_listings_for_curation()
    assert {r["id"] for r in rows} == {"booking_1", "airbnb_2"}
    assert any(r["business_phone"] for r in rows)

    db.set_operator_ids({"booking_1": "op_x", "airbnb_2": "op_x"})
    db.set_property_groups({"booking_1": "pg_1", "airbnb_2": "pg_1"}, cross_platform={"pg_1"})
    got = {r[0]: tuple(r[1:]) for r in db.conn.execute(
        "SELECT id, operator_id, property_group_id, cross_platform_group_id FROM listings")}
    assert got["booking_1"] == ("op_x", "pg_1", "pg_1")
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_database_curation.py -v`
Expected: FAIL (new columns/tables/methods missing).

- [ ] **Step 3: Extend the migration**

In `src/storage/database.py`, inside `_migrate`, add these entries to the `new_columns` list (after `("first_seen_at", "TEXT")`):

```python
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
```

Then, just before the final `self.conn.commit()` in `_migrate`, create the two new tables:

```python
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
                last_tried_at TEXT,
                raw_json TEXT
            );
        """)
```

- [ ] **Step 4: Add curation reader + writers**

Append these methods to the `Database` class (before `close`):

```python
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

    def set_property_groups(self, mapping: dict[str, str],
                            cross_platform: set[str]) -> int:
        """Write property_group_id; set cross_platform_group_id to the group id
        only for groups in `cross_platform` (those spanning both platforms)."""
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

    def clear_position_observations(self) -> None:
        self.conn.execute("DELETE FROM position_observations")
        self.conn.commit()

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
```

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_database_curation.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add src/storage/database.py tests/test_database_curation.py
git commit -m "feat: curation schema (geo columns, observation ledger, geocode cache) + readers/writers"
```

---

## Task 4: Layered property dedup (Tier 0/1/2)

**Files:**
- Create: `src/dedup/property_groups.py`
- Test: `tests/test_property_groups.py`

- [ ] **Step 1: Write failing tests**

`tests/test_property_groups.py`:

```python
from src.dedup.property_groups import room_config_matches, assign_property_groups


def _row(id, platform, name, lat, lng, reg=None, phone=None, email=None,
         bedrooms=None, beds=None, bathrooms=None):
    return {"id": id, "platform": platform, "name": name,
            "latitude": lat, "longitude": lng,
            "business_registration_number": reg, "business_phone": phone,
            "business_email": email, "bedrooms": bedrooms, "beds": beds,
            "bathrooms": bathrooms}


def test_room_config_matches():
    assert room_config_matches({"bedrooms": 2, "beds": 3, "bathrooms": 1},
                               {"bedrooms": 2, "beds": None, "bathrooms": 1})
    assert not room_config_matches({"bedrooms": 2}, {"bedrooms": 3})
    assert not room_config_matches({"bedrooms": None}, {"bedrooms": None})  # nothing comparable


def test_tier0_singleton_identity_links_despite_distance():
    # Same unique phone, but Airbnb point fuzzed ~400 m away and a different name.
    rows = [
        _row("booking_1", "booking", "Central Studio", 44.4300, 26.1000, phone="0721000111"),
        _row("airbnb_2", "airbnb", "Cozy Place Downtown", 44.4336, 26.1000, phone="+40721000111"),
    ]
    mapping, cross, _ident = assign_property_groups(rows, operator_map={"booking_1": "op_a", "airbnb_2": "op_a"})
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping["booking_1"] in cross  # spans both platforms


def test_shared_operator_many_units_does_not_overmerge():
    # One operator (shared phone) with two genuinely different flats far apart.
    rows = [
        _row("booking_1", "booking", "Flat A Unirii", 44.4270, 26.1020, phone="0720000001"),
        _row("airbnb_2", "airbnb", "Flat A Unirii", 44.4270, 26.1020, phone="0720000001"),
        _row("booking_3", "booking", "Flat B Aviatorilor", 44.4700, 26.0870, phone="0720000001"),
    ]
    op = {"booking_1": "op_a", "airbnb_2": "op_a", "booking_3": "op_a"}
    mapping, cross, _ident = assign_property_groups(rows, operator_map=op)
    # The two co-located A flats group; the distant B flat must NOT join them.
    assert mapping["booking_1"] == mapping["airbnb_2"]
    assert mapping.get("booking_3") != mapping["booking_1"]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_property_groups.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/dedup/property_groups.py`:

```python
from __future__ import annotations

import hashlib
from collections import defaultdict

from rapidfuzz import fuzz

from .deduplicator import haversine_distance
from .operators import normalize_registration, normalize_phone, normalize_email

# Tier thresholds (overridable via config in the curation stage).
TIER1_RELAXED_DISTANCE_M = 250.0
TIER1_NAME_THRESHOLD = 70.0
TIER2_STRICT_DISTANCE_M = 100.0
TIER2_NAME_THRESHOLD = 80.0


def room_config_matches(a: dict, b: dict) -> bool:
    """True if every room field present on BOTH sides is equal, and at least one
    field is comparable. None is treated as wildcard."""
    comparable = 0
    for field in ("bedrooms", "beds", "bathrooms"):
        av, bv = a.get(field), b.get(field)
        if av is None or bv is None:
            continue
        comparable += 1
        if av != bv:
            return False
    return comparable > 0


def _name_sim(a: dict, b: dict) -> float:
    return fuzz.ratio((a.get("name") or "").lower(), (b.get("name") or "").lower())


def _identity_keys(row: dict) -> set[str]:
    keys = set()
    for fn, field in (
        (normalize_registration, "business_registration_number"),
        (normalize_phone, "business_phone"),
        (normalize_email, "business_email"),
    ):
        k = fn(row.get(field))
        if k:
            keys.add(field[:3] + ":" + k)  # namespaced so phone != reg collision-free
    return keys


def _compatible(a: dict, b: dict, relaxed: bool) -> bool:
    """Clique-compatibility predicate used to guard group growth."""
    if _identity_keys(a) & _identity_keys(b):
        return True
    dist = haversine_distance(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
    if relaxed:
        return dist <= TIER1_RELAXED_DISTANCE_M and (
            _name_sim(a, b) >= TIER1_NAME_THRESHOLD or room_config_matches(a, b))
    return dist <= TIER2_STRICT_DISTANCE_M and _name_sim(a, b) >= TIER2_NAME_THRESHOLD


def assign_property_groups(rows, operator_map: dict[str, str]):
    """Group listings that are the same physical flat (within or across
    platforms). Returns (mapping, cross_platform_groups, identity_groups):
      mapping: {listing_id: group_id} for listings in a multi-member group
      cross_platform_groups: set of group_ids whose members span both platforms
      identity_groups: group_ids formed using identity/operator signals (Tier
        0/1) — excluded from the verification precision proxy to avoid circularity

    Three candidate tiers, highest confidence first:
      Tier 0: an identity key (phone/email/reg) mapping to exactly one Booking
              and exactly one Airbnb listing -> direct link (single-property host).
      Tier 1: within a shared operator block, relaxed distance + name/room match.
      Tier 2: outside operator blocks, tightened distance + name.
    Greedy with a clique-compatibility check so groups can't chain distant flats.
    """
    by_id = {r["id"]: r for r in rows}

    # ---- Tier 0: singleton identity across platforms ----
    key_to_booking: dict[str, list[str]] = defaultdict(list)
    key_to_airbnb: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        for k in _identity_keys(r):
            (key_to_booking if r["platform"] == "booking" else key_to_airbnb)[k].append(r["id"])

    candidates: list[tuple[int, float, str, str]] = []  # (tier, -confidence, id_a, id_b)
    for k in set(key_to_booking) & set(key_to_airbnb):
        if len(key_to_booking[k]) == 1 and len(key_to_airbnb[k]) == 1:
            candidates.append((0, -1.0, key_to_booking[k][0], key_to_airbnb[k][0]))

    # ---- Tier 1: within operator blocks ----
    blocks: dict[str, list[str]] = defaultdict(list)
    for lid, op in operator_map.items():
        if lid in by_id:
            blocks[op].append(lid)
    for members in blocks.values():
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a, b = by_id[members[i]], by_id[members[j]]
                if _compatible(a, b, relaxed=True):
                    candidates.append((1, -_name_sim(a, b) / 100.0, a["id"], b["id"]))

    # ---- Tier 2: spatial bucket (tightened) ----
    buckets: dict[tuple[float, float], list[str]] = defaultdict(list)
    for r in rows:
        buckets[(round(r["latitude"], 3), round(r["longitude"], 3))].append(r["id"])
    for r in rows:
        blat, blng = round(r["latitude"], 3), round(r["longitude"], 3)
        for dlat in (-0.001, 0.0, 0.001):
            for dlng in (-0.001, 0.0, 0.001):
                for other_id in buckets.get((round(blat + dlat, 3), round(blng + dlng, 3)), ()):
                    if other_id <= r["id"]:
                        continue
                    a, b = r, by_id[other_id]
                    if _compatible(a, b, relaxed=False):
                        candidates.append((2, -_name_sim(a, b) / 100.0, a["id"], b["id"]))

    candidates.sort()  # tier asc, then -confidence asc (best first)

    # ---- Greedy union with clique check ----
    groups: dict[str, set[str]] = {}     # temp group_id -> members
    of: dict[str, str] = {}              # listing_id -> temp group_id
    identity_tmp: set[str] = set()       # temp gids formed using identity/operator (tier 0/1)
    counter = 0

    for tier, _conf, a_id, b_id in candidates:
        relaxed = tier != 2
        ga, gb = of.get(a_id), of.get(b_id)
        if ga and ga == gb:
            continue
        target: str | None = None
        if ga is None and gb is None:
            counter += 1
            target = f"_tmp_{counter}"
            groups[target] = {a_id, b_id}
            of[a_id] = of[b_id] = target
        elif ga and gb is None:
            if all(_compatible(by_id[b_id], by_id[m], relaxed) for m in groups[ga]):
                groups[ga].add(b_id)
                of[b_id] = ga
                target = ga
        elif gb and ga is None:
            if all(_compatible(by_id[a_id], by_id[m], relaxed) for m in groups[gb]):
                groups[gb].add(a_id)
                of[a_id] = gb
                target = gb
        else:  # both in different groups
            if all(_compatible(by_id[x], by_id[y], relaxed)
                   for x in groups[ga] for y in groups[gb]):
                gb_ident = gb in identity_tmp
                groups[ga] |= groups[gb]
                for m in groups[gb]:
                    of[m] = ga
                del groups[gb]
                identity_tmp.discard(gb)
                if gb_ident:
                    identity_tmp.add(ga)
                target = ga
        if target is not None and tier in (0, 1):
            identity_tmp.add(target)

    # ---- Stable ids + cross-platform / identity flags ----
    mapping: dict[str, str] = {}
    cross: set[str] = set()
    identity_groups: set[str] = set()
    for tmp_gid, members in groups.items():
        gid = "pg_" + hashlib.md5("|".join(sorted(members)).encode()).hexdigest()[:16]
        platforms = {by_id[m]["platform"] for m in members}
        for m in members:
            mapping[m] = gid
        if {"booking", "airbnb"} <= platforms:
            cross.add(gid)
        if tmp_gid in identity_tmp:
            identity_groups.add(gid)
    return mapping, cross, identity_groups
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_property_groups.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dedup/property_groups.py tests/test_property_groups.py
git commit -m "feat: layered (Tier 0/1/2) property dedup with clique-guarded greedy union"
```

---

## Task 5: Precision classification + Booking address extraction

**Files:**
- Create: `src/geo/__init__.py` (empty), `src/geo/precision.py`
- Test: `tests/test_precision.py`

- [ ] **Step 1: Write failing tests**

`tests/test_precision.py`:

```python
from src.geo.precision import extract_booking_address, classify_scraped_precision

BK = '{"location": {"address": "36 Strada Moise Nicoara bloc D2, apartament 56", "city": "Bucuresti"}}'
BK_VAGUE = '{"location": {"address": "Sector 3", "city": "Bucuresti"}}'


def test_extract_booking_address():
    assert extract_booking_address(BK) == "36 Strada Moise Nicoara bloc D2, apartament 56, Bucuresti"
    assert extract_booking_address('{"x":1}') is None


def test_classify_booking_street_level():
    prec, sigma = classify_scraped_precision({"platform": "booking", "raw_json": BK}, stack_count=1)
    assert sigma == 50.0


def test_classify_booking_vague_or_stacked():
    _, sigma = classify_scraped_precision({"platform": "booking", "raw_json": BK_VAGUE}, stack_count=1)
    assert sigma == 150.0
    _, sigma_stacked = classify_scraped_precision({"platform": "booking", "raw_json": BK}, stack_count=5)
    assert sigma_stacked == 150.0


def test_classify_airbnb_always_fuzzed():
    _, sigma = classify_scraped_precision({"platform": "airbnb", "raw_json": None}, stack_count=1)
    assert sigma == 100.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_precision.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/geo/__init__.py`: empty file.

`src/geo/precision.py`:

```python
from __future__ import annotations

import json
import re

# Default per-source sigmas (metres). Overridable via FusionConfig.
SIGMA_GEOCODED = 25.0
SIGMA_BOOKING_ADDRESS = 50.0
SIGMA_VAGUE = 150.0
SIGMA_AIRBNB = 100.0

_STREET_RE = re.compile(
    r"\d|strada|str\.|calea|bulevardul|bd\.|soseaua|sos\.|aleea|bloc|apartament|ap\.",
    re.IGNORECASE,
)


def extract_booking_address(raw_json: str | None) -> str | None:
    """Pull 'address, city' from a Booking raw_json location block."""
    if not raw_json:
        return None
    try:
        loc = (json.loads(raw_json) or {}).get("location") or {}
    except (ValueError, AttributeError):
        return None
    address = (loc.get("address") or "").strip()
    if not address:
        return None
    city = (loc.get("city") or "").strip()
    return f"{address}, {city}" if city else address


def _is_street_level(address: str | None) -> bool:
    return bool(address and _STREET_RE.search(address))


def classify_scraped_precision(row: dict, stack_count: int) -> tuple[str, float]:
    """Return (provisional_precision, sigma_m) for a listing's scraped coordinate.

    The sigma seeds the observation ledger; the listing's authoritative
    location_precision is decided later by fusion. `stack_count` is how many
    listings share this exact coordinate (>=3 => centroid => approximate)."""
    if row["platform"] == "booking":
        address = extract_booking_address(row.get("raw_json"))
        if _is_street_level(address) and stack_count < 3:
            return "approximate", SIGMA_BOOKING_ADDRESS
        return "approximate", SIGMA_VAGUE
    # Airbnb: fuzzed by policy.
    return "approximate", SIGMA_AIRBNB
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_precision.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/geo/__init__.py src/geo/precision.py tests/test_precision.py
git commit -m "feat: scraped-coordinate precision classification + Booking address extraction"
```

---

## Task 6: Coordinate fusion

**Files:**
- Create: `src/geo/fusion.py`
- Test: `tests/test_fusion.py`

- [ ] **Step 1: Write failing tests**

`tests/test_fusion.py`:

```python
from src.geo.fusion import Observation, fuse_observations, position_confidence


def test_precise_observation_dominates_fuzzed_one():
    obs = [
        Observation("booking_1", 44.4300, 26.1000, 25.0, "geocoded"),   # precise
        Observation("airbnb_2", 44.4330, 26.1000, 100.0, "scraped"),    # fuzzed ~330 m N
    ]
    fused = fuse_observations(obs)
    # Result should sit very close to the precise point, not the midpoint.
    assert abs(fused.latitude - 44.4300) < 0.0005
    assert fused.sigma_m < 25.0  # fusing reduces uncertainty
    assert fused.dominant_listing_id == "booking_1"


def test_two_equal_approximate_points_reduce_sigma():
    obs = [
        Observation("a", 44.4300, 26.1000, 100.0, "scraped"),
        Observation("b", 44.4300, 26.1000, 100.0, "scraped"),
    ]
    fused = fuse_observations(obs)
    assert abs(fused.sigma_m - 70.7) < 1.0  # 100/sqrt(2)


def test_outlier_rejected():
    obs = [
        Observation("a", 44.4300, 26.1000, 50.0, "geocoded"),
        Observation("b", 44.4302, 26.1001, 50.0, "scraped"),
        Observation("c", 45.0000, 27.0000, 100.0, "scraped"),  # >1 km outlier
    ]
    fused = fuse_observations(obs)
    assert fused.latitude < 44.45  # the outlier did not drag it north


def test_confidence_monotonic():
    assert position_confidence(20.0) > position_confidence(120.0)
    assert 0.0 <= position_confidence(500.0) <= 1.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_fusion.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/geo/fusion.py`:

```python
from __future__ import annotations

import math
from dataclasses import dataclass

# Local equirectangular projection anchored on central Bucharest.
_LAT0, _LNG0 = 44.4325, 26.1000
_DEG_LAT_M = 111_320.0
_DEG_LNG_M = 111_320.0 * math.cos(math.radians(_LAT0))  # ~79,545 m

_OUTLIER_M = 1000.0


@dataclass
class Observation:
    listing_id: str
    latitude: float
    longitude: float
    sigma_m: float
    source: str  # 'scraped' | 'geocoded'


@dataclass
class FusedPosition:
    latitude: float
    longitude: float
    sigma_m: float
    dominant_listing_id: str
    dominant_source: str


def _to_local(lat: float, lng: float) -> tuple[float, float]:
    return ((lng - _LNG0) * _DEG_LNG_M, (lat - _LAT0) * _DEG_LAT_M)


def _to_geo(x: float, y: float) -> tuple[float, float]:
    return (_LAT0 + y / _DEG_LAT_M, _LNG0 + x / _DEG_LNG_M)


def _weighted_mean(obs: list[Observation]) -> tuple[float, float, float]:
    sw = sum(1.0 / (o.sigma_m ** 2) for o in obs)
    x = sum((1.0 / o.sigma_m ** 2) * _to_local(o.latitude, o.longitude)[0] for o in obs) / sw
    y = sum((1.0 / o.sigma_m ** 2) * _to_local(o.latitude, o.longitude)[1] for o in obs) / sw
    sigma = 1.0 / math.sqrt(sw)
    return x, y, sigma


def fuse_observations(observations: list[Observation]) -> FusedPosition:
    """Inverse-variance weighted fusion with >1 km outlier rejection."""
    obs = [o for o in observations if o.sigma_m and o.sigma_m > 0]
    if not obs:
        raise ValueError("no usable observations")

    x0, y0, _ = _weighted_mean(obs)
    kept = [o for o in obs
            if math.dist((x0, y0), _to_local(o.latitude, o.longitude)) <= _OUTLIER_M]
    if not kept:
        kept = obs
    x, y, sigma = _weighted_mean(kept)
    lat, lng = _to_geo(x, y)

    dominant = min(kept, key=lambda o: o.sigma_m)  # smallest sigma == largest weight
    return FusedPosition(lat, lng, sigma, dominant.listing_id, dominant.source)


def position_confidence(sigma_m: float) -> float:
    """Map a fused sigma to a 0-1 confidence (1 at 0 m, 0 at >=150 m)."""
    return max(0.0, min(1.0, (150.0 - sigma_m) / 150.0))
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_fusion.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/geo/fusion.py tests/test_fusion.py
git commit -m "feat: inverse-variance coordinate fusion with outlier rejection"
```

---

## Task 7: Geocoding client + DB-backed cache with persistent retry

**Files:**
- Create: `src/geo/geocode.py`
- Test: `tests/test_geocode.py`

- [ ] **Step 1: Write failing tests** (HTTP injected, so no network)

`tests/test_geocode.py`:

```python
from src.geo.geocode import normalize_address, Geocoder


def test_normalize_address():
    assert normalize_address("  36 Strada X,  Bucuresti ") == "36 strada x, bucuresti"


def test_geocoder_caches_success(db):
    calls = {"n": 0}

    def fake_fetch(query):
        calls["n"] += 1
        return [{"lat": "44.43", "lon": "26.10", "category": "building"}]

    g = Geocoder(db, fetch_fn=fake_fetch, rate_limit_s=0, max_retries=5)
    r1 = g.geocode("36 Strada X, Bucuresti")
    r2 = g.geocode("36 Strada X, Bucuresti")  # served from cache
    assert r1 == (44.43, 26.10)
    assert r2 == (44.43, 26.10)
    assert calls["n"] == 1  # second call hit the cache


def test_geocoder_retries_failures_until_cap(db):
    def failing_fetch(query):
        raise TimeoutError("boom")

    g = Geocoder(db, fetch_fn=failing_fetch, rate_limit_s=0, max_retries=3)
    assert g.geocode("X, Bucuresti") is None
    cached = db.get_geocode(normalize_address("X, Bucuresti"))
    assert cached["status"] == "failed"
    assert cached["attempts"] == 1  # one attempt this run; re-tried on later runs

    # A later run retries (attempts increments) until it reaches the cap.
    g.geocode("X, Bucuresti")
    assert db.get_geocode(normalize_address("X, Bucuresti"))["attempts"] == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_geocode.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/geo/geocode.py`:

```python
from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


def normalize_address(address: str) -> str:
    """Cache key: lowercased, whitespace-collapsed."""
    return " ".join(address.lower().split())


def _http_fetch(query: str, base_url: str, user_agent: str, timeout: int) -> list[dict]:
    params = urllib.parse.urlencode(
        {"q": query, "format": "jsonv2", "limit": 1, "countrycodes": "ro"})
    req = urllib.request.Request(f"{base_url}?{params}", headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


class Geocoder:
    """Forward-geocode addresses via Nominatim with a DB-backed cache and
    persistent retry across runs. The HTTP call is injectable for testing."""

    def __init__(self, db, fetch_fn=None, base_url: str = _NOMINATIM_URL,
                 user_agent: str = "bucharest-str-research/1.0", rate_limit_s: float = 1.0,
                 timeout: int = 20, max_retries: int = 5):
        self.db = db
        self.base_url = base_url
        self.user_agent = user_agent
        self.rate_limit_s = rate_limit_s
        self.timeout = timeout
        self.max_retries = max_retries
        self._fetch_fn = fetch_fn or (
            lambda q: _http_fetch(q, self.base_url, self.user_agent, self.timeout))
        self._last_call = 0.0

    def _throttle(self):
        if self.rate_limit_s:
            wait = self.rate_limit_s - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
        self._last_call = time.monotonic()

    def geocode(self, address: str) -> tuple[float, float] | None:
        """Return (lat, lng) or None. Caches successes forever; failures are
        retried on each run until `max_retries` attempts accumulate."""
        key = normalize_address(address)
        cached = self.db.get_geocode(key)
        if cached:
            if cached["status"] == "ok":
                return (cached["latitude"], cached["longitude"])
            if cached["status"] == "not_found" or cached["attempts"] >= self.max_retries:
                return None
        attempts = (cached["attempts"] if cached else 0)

        self._throttle()
        try:
            results = self._fetch_fn(address)
        except Exception as e:  # network/timeout/parse — transient, retry next run
            logger.warning("Geocode failed for %r: %s", address, e)
            self.db.upsert_geocode(key, "failed", None, None, None, attempts + 1)
            return None

        if not results:
            self.db.upsert_geocode(key, "not_found", None, None, None, attempts + 1)
            return None
        top = results[0]
        lat, lng = float(top["lat"]), float(top["lon"])
        self.db.upsert_geocode(key, "ok", lat, lng, top.get("category"), attempts + 1)
        return (lat, lng)
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_geocode.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/geo/geocode.py tests/test_geocode.py
git commit -m "feat: Nominatim geocoder with DB-backed cache + persistent retry"
```

---

## Task 8: Dedup verification metrics

**Files:**
- Create: `src/dedup/validate.py`
- Test: `tests/test_validate.py`

- [ ] **Step 1: Write failing test**

`tests/test_validate.py`:

```python
from src.dedup.validate import dedup_metrics


def _row(id, platform, reg=None, phone=None):
    return {"id": id, "platform": platform,
            "business_registration_number": reg, "business_phone": phone}


def test_metrics_flag_identity_conflict():
    rows = [
        _row("booking_1", "booking", reg="RO111"),
        _row("airbnb_2", "airbnb", reg="RO111"),   # agrees -> good
        _row("booking_3", "booking", reg="RO222"),
        _row("airbnb_4", "airbnb", reg="RO999"),    # conflict within a group
    ]
    # Tier 1/2 groups (proximity/name matched): {1,2} agree, {3,4} conflict.
    mapping = {"booking_1": "pg_a", "airbnb_2": "pg_a",
               "booking_3": "pg_b", "airbnb_4": "pg_b"}
    excluded_groups = set()  # both groups matched by proximity/name (Tier 2)
    m = dedup_metrics(rows, mapping, excluded_groups)
    assert m["comparable_groups"] == 2
    assert m["agreeing_groups"] == 1
    assert m["precision_proxy"] == 0.5
    assert "pg_b" in m["conflict_groups"]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_validate.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/dedup/validate.py`:

```python
from __future__ import annotations

from collections import defaultdict

from .operators import normalize_registration, normalize_phone, normalize_email


def _identity_keys(row: dict) -> set[str]:
    keys = set()
    for fn, field in (
        (normalize_registration, "business_registration_number"),
        (normalize_phone, "business_phone"),
        (normalize_email, "business_email"),
    ):
        k = fn(row.get(field))
        if k:
            keys.add(field[:3] + ":" + k)
    return keys


def dedup_metrics(rows, mapping: dict[str, str], excluded_groups: set[str]) -> dict:
    """Identity-key ground-truth check on proximity/name (Tier 2) property groups.

    A group is 'comparable' if >=2 of its members carry any identity key. It
    'agrees' if all such members share at least one key; otherwise it is a
    conflict (false-positive suspect). Identity/operator-derived groups (Tier
    0/1, passed as `excluded_groups`) are excluded to avoid circularity. Returns
    precision/recall proxies + conflicts.
    """
    by_id = {r["id"]: r for r in rows}
    members: dict[str, list[str]] = defaultdict(list)
    for lid, gid in mapping.items():
        members[gid].append(lid)

    comparable = agreeing = 0
    conflicts: list[str] = []
    for gid, ids in members.items():
        if gid in excluded_groups:
            continue
        keyed = [by_id[i] for i in ids if i in by_id and _identity_keys(by_id[i])]
        if len(keyed) < 2:
            continue
        comparable += 1
        common = set.intersection(*[_identity_keys(r) for r in keyed])
        if common:
            agreeing += 1
        else:
            conflicts.append(gid)

    # Recall proxy: identity-confirmed cross-platform twins we DID group.
    key_pairs = defaultdict(lambda: {"booking": set(), "airbnb": set()})
    for r in rows:
        for k in _identity_keys(r):
            key_pairs[k][r["platform"]].add(r["id"])
    singleton_twins = [
        (next(iter(v["booking"])), next(iter(v["airbnb"])))
        for v in key_pairs.values()
        if len(v["booking"]) == 1 and len(v["airbnb"]) == 1
    ]
    grouped = sum(1 for b, a in singleton_twins
                  if mapping.get(b) and mapping.get(b) == mapping.get(a))

    return {
        "comparable_groups": comparable,
        "agreeing_groups": agreeing,
        "precision_proxy": round(agreeing / comparable, 4) if comparable else None,
        "conflict_groups": conflicts,
        "identity_twins": len(singleton_twins),
        "identity_twins_grouped": grouped,
        "recall_proxy": round(grouped / len(singleton_twins), 4) if singleton_twins else None,
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_validate.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dedup/validate.py tests/test_validate.py
git commit -m "feat: dedup verification metrics from identity-key ground truth"
```

---

## Task 9: Config — geocoding / dedup / fusion sections

**Files:**
- Modify: `config/scraping.yaml`, `src/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing test**

`tests/test_config.py`:

```python
from src.config import load_config


def test_curation_config_loads_with_defaults():
    cfg = load_config()
    assert cfg.geocoding.enabled in (True, False)
    assert cfg.geocoding.rate_limit_s >= 0
    assert cfg.dedup.strict_distance_m == 100
    assert cfg.fusion.sigma_geocoded_m == 25
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL (`cfg.geocoding` missing).

- [ ] **Step 3: Add YAML**

Append to `config/scraping.yaml`:

```yaml
geocoding:
  enabled: true
  nominatim_url: "https://nominatim.openstreetmap.org/search"
  user_agent: "bucharest-str-research/1.0 (mihaiurudolph@gmail.com)"
  rate_limit_s: 1.0
  timeout_seconds: 20
  max_retries: 5

dedup:
  operator_relaxed_distance_m: 250
  strict_distance_m: 100
  strict_name_threshold: 80

fusion:
  sigma_geocoded_m: 25
  sigma_booking_address_m: 50
  sigma_vague_m: 150
  sigma_airbnb_m: 100
  disagreement_km: 1.0
  exact_max_sigma_m: 40
```

- [ ] **Step 4: Add dataclasses + loading to `src/config.py`**

Add three dataclasses after `ScrapingConfig`:

```python
@dataclass
class GeocodingConfig:
    enabled: bool = True
    nominatim_url: str = "https://nominatim.openstreetmap.org/search"
    user_agent: str = "bucharest-str-research/1.0"
    rate_limit_s: float = 1.0
    timeout_seconds: int = 20
    max_retries: int = 5


@dataclass
class DedupConfig:
    operator_relaxed_distance_m: float = 250.0
    strict_distance_m: float = 100.0
    strict_name_threshold: float = 80.0


@dataclass
class FusionConfig:
    sigma_geocoded_m: float = 25.0
    sigma_booking_address_m: float = 50.0
    sigma_vague_m: float = 150.0
    sigma_airbnb_m: float = 100.0
    disagreement_km: float = 1.0
    exact_max_sigma_m: float = 40.0
```

Add three fields to `AppConfig`:

```python
@dataclass
class AppConfig:
    city: CityConfig
    scraping: ScrapingConfig
    proxy_urls: list[str]
    geocoding: GeocodingConfig
    dedup: DedupConfig
    fusion: FusionConfig
```

In `load_config`, before the final `return`, build them and pass them in:

```python
    geocoding = GeocodingConfig(**{**GeocodingConfig().__dict__, **scraping_raw.get("geocoding", {})})
    dedup = DedupConfig(**{**DedupConfig().__dict__, **scraping_raw.get("dedup", {})})
    fusion = FusionConfig(**{**FusionConfig().__dict__, **scraping_raw.get("fusion", {})})

    return AppConfig(city=city, scraping=scraping, proxy_urls=proxy_urls,
                     geocoding=geocoding, dedup=dedup, fusion=fusion)
```

(Replace the existing single-line `return AppConfig(...)`.)

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add src/config.py config/scraping.yaml tests/test_config.py
git commit -m "feat: geocoding/dedup/fusion config sections"
```

---

## Task 10: Curation orchestration — `run_curation()`

**Files:**
- Create: `src/geo/curate.py`
- Test: `tests/test_curate.py`

- [ ] **Step 1: Write failing integration test**

`tests/test_curate.py`:

```python
from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing
from src.geo.curate import run_curation


def _mk(db, lid, platform, name, lat, lng, **kw):
    db.upsert_listing(Listing(
        id=lid, platform=platform, platform_id=lid.split("_")[1], name=name,
        latitude=lat, longitude=lng, scraped_at=datetime(2026, 5, 15),
        first_seen_at=datetime(2026, 5, 15), **kw))


def test_run_curation_links_twin_and_improves_airbnb_position(db):
    # Booking (precise street address) + Airbnb (fuzzed ~330 m, same phone).
    _mk(db, "booking_1", Platform.BOOKING, "Central Studio", 44.4300, 26.1000,
        business_phone="0721000111",
        raw_json='{"location":{"address":"5 Strada Lipscani","city":"Bucuresti"}}')
    _mk(db, "airbnb_2", Platform.AIRBNB, "Cozy Downtown", 44.4330, 26.1000,
        business_phone="+40721000111")

    # Inject a geocoder that returns a precise point for the Booking address.
    def fake_fetch(query):
        return [{"lat": "44.4301", "lon": "26.1001", "category": "building"}]

    metrics = run_curation(db, fetch_fn=fake_fetch)

    rows = {r[0]: r for r in db.conn.execute(
        "SELECT id, property_group_id, latitude_best, longitude_best, location_source, position_confidence FROM listings")}
    # Twin linked
    assert rows["booking_1"][1] == rows["airbnb_2"][1] is not None
    # Airbnb best position pulled toward the geocoded Booking point (was 44.4330)
    assert rows["airbnb_2"][2] < 44.4320
    assert rows["airbnb_2"][4] == "transferred_from_twin"
    assert rows["booking_1"][4] == "geocoded_address"
    assert metrics["identity_twins_grouped"] == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_curate.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

`src/geo/curate.py`:

```python
from __future__ import annotations

import logging
from collections import Counter, defaultdict

from ..dedup.operators import assign_operator_ids
from ..dedup.property_groups import assign_property_groups
from ..dedup.validate import dedup_metrics
from .fusion import Observation, fuse_observations, position_confidence
from .geocode import Geocoder
from .precision import (
    classify_scraped_precision, extract_booking_address,
    SIGMA_GEOCODED,
)

logger = logging.getLogger(__name__)


def run_curation(db, config=None, fetch_fn=None, backfill_rows=None) -> dict:
    """Run the full geo/dedup curation stage on the DB. Returns dedup metrics.

    Steps: operators -> property groups -> precision observations -> geocode ->
    fusion -> verification. `fetch_fn` injects the geocoder HTTP for tests;
    `backfill_rows` is an optional list of historical observation tuples
    (listing_id, lat, lng, sigma_m, capture_date, platform) from a prior DB."""
    rows = db.get_listings_for_curation()
    if not rows:
        logger.info("Curation: no listings.")
        return {}
    by_id = {r["id"]: r for r in rows}

    # 1. Operators
    operator_map = assign_operator_ids(rows)
    db.set_operator_ids(operator_map)
    logger.info("Curation: %d listings carry an operator_id", len(operator_map))

    # 2. Property groups
    group_map, cross_groups, identity_groups = assign_property_groups(rows, operator_map)
    db.set_property_groups(group_map, cross_groups)
    logger.info("Curation: %d listings in %d property groups (%d cross-platform)",
                len(group_map), len(set(group_map.values())), len(cross_groups))

    # 3. Geocode Booking addresses + collect scraped/geocoded observations.
    geocfg = getattr(config, "geocoding", None)
    geocoder = Geocoder(
        db, fetch_fn=fetch_fn,
        base_url=getattr(geocfg, "nominatim_url", "https://nominatim.openstreetmap.org/search"),
        user_agent=getattr(geocfg, "user_agent", "bucharest-str-research/1.0"),
        rate_limit_s=getattr(geocfg, "rate_limit_s", 1.0),
        timeout=getattr(geocfg, "timeout_seconds", 20),
        max_retries=getattr(geocfg, "max_retries", 5),
    ) if (geocfg is None or geocfg.enabled) else None

    stack = Counter((round(r["latitude"], 6), round(r["longitude"], 6)) for r in rows)

    db.clear_position_observations()
    observations: list[tuple] = []   # for the ledger
    fuse_inputs: dict[str, list[Observation]] = defaultdict(list)  # group_key -> Observation
    geocoded_map: dict[str, tuple] = {}

    def group_key(lid: str) -> str:
        return group_map.get(lid, lid)  # singletons fuse on their own id

    for r in rows:
        lid = r["id"]
        gk = group_key(lid)
        _, sigma = classify_scraped_precision(
            r, stack[(round(r["latitude"], 6), round(r["longitude"], 6))])
        cap_date = (r.get("scraped_at") or "")[:10]
        observations.append((lid, group_map.get(lid), cap_date, r["platform"],
                             "scraped", r["latitude"], r["longitude"], sigma))
        fuse_inputs[gk].append(Observation(lid, r["latitude"], r["longitude"], sigma, "scraped"))

        if geocoder and r["platform"] == "booking":
            address = extract_booking_address(r.get("raw_json"))
            if address:
                hit = geocoder.geocode(address)
                if hit:
                    geocoded_map[lid] = (hit[0], hit[1], address)
                    observations.append((lid, group_map.get(lid), cap_date, r["platform"],
                                        "geocoded", hit[0], hit[1], SIGMA_GEOCODED))
                    fuse_inputs[gk].append(Observation(lid, hit[0], hit[1], SIGMA_GEOCODED, "geocoded"))

    # 3b. Temporal backfill (historical observations from a prior capture).
    for (lid, lat, lng, sigma_m, cap_date, platform) in (backfill_rows or []):
        if lid in by_id:
            gk = group_key(lid)
            observations.append((lid, group_map.get(lid), cap_date, platform, "scraped", lat, lng, sigma_m))
            fuse_inputs[gk].append(Observation(lid, lat, lng, sigma_m, "scraped"))

    db.add_position_observations(observations)
    db.set_geocoded(geocoded_map)

    # 4. Fuse each group; every member of a group gets the same best position.
    members_by_key: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        members_by_key[group_key(r["id"])].append(r["id"])

    fused_map: dict[str, dict] = {}
    exact_max = getattr(getattr(config, "fusion", None), "exact_max_sigma_m", 40.0)
    for gk, obs in fuse_inputs.items():
        fused = fuse_observations(obs)
        for lid in members_by_key.get(gk, []):
            if fused.dominant_listing_id != lid:
                source = "transferred_from_twin"
            elif fused.dominant_source == "geocoded":
                source = "geocoded_address"
            else:
                source = "platform_coord"
            fused_map[lid] = {
                "lat_best": fused.latitude, "lng_best": fused.longitude,
                "est_accuracy_m": round(fused.sigma_m, 1),
                "position_confidence": round(position_confidence(fused.sigma_m), 3),
                "location_source": source,
                "location_precision": "exact" if fused.sigma_m <= exact_max else "approximate",
            }
    db.set_fused_positions(fused_map)

    # 5. Verification (exclude identity/operator-derived groups to avoid circularity)
    metrics = dedup_metrics(rows, group_map, identity_groups)
    logger.info("Curation metrics: %s", metrics)
    return metrics
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_curate.py -v`
Expected: 1 passed.

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest tests/ -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/geo/curate.py tests/test_curate.py
git commit -m "feat: run_curation stage wiring operators->dedup->geocode->fusion->metrics"
```

---

## Task 11: Backfill reader + exporter updates

**Files:**
- Modify: `src/storage/database.py` (add `read_historical_observations` classmethod-style helper), `src/storage/exporter.py`
- Test: `tests/test_exporter.py`

- [ ] **Step 1: Write failing test**

`tests/test_exporter.py`:

```python
import csv
from datetime import datetime

from src.models.enums import Platform
from src.models.listing import Listing
from src.storage.exporter import export_csv, export_geojson


def _mk(db, lid, platform, lat, lng, best=None):
    db.upsert_listing(Listing(id=lid, platform=platform, platform_id=lid.split("_")[1],
                              name="X", latitude=lat, longitude=lng,
                              scraped_at=datetime(2026, 5, 15), first_seen_at=datetime(2026, 5, 15)))
    if best:
        db.conn.execute("UPDATE listings SET latitude_best=?, longitude_best=? WHERE id=?",
                        (best[0], best[1], lid))
        db.conn.commit()


def test_csv_has_new_columns(db, tmp_path):
    _mk(db, "booking_1", Platform.BOOKING, 44.43, 26.10, best=(44.4301, 26.1001))
    path = export_csv(db, output_path=tmp_path / "l.csv")
    with open(path, encoding="utf-8-sig") as f:
        header = next(csv.reader(f))
    for c in ("operator_id", "property_group_id", "latitude_best", "location_precision",
              "position_confidence"):
        assert c in header


def test_geojson_uses_best_coords_when_present(db, tmp_path):
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10, best=(44.4301, 26.1001))
    import json
    path = export_geojson(db, output_path=tmp_path / "l.geojson")
    feat = json.load(open(path, encoding="utf-8"))["features"][0]
    assert feat["geometry"]["coordinates"] == [26.1001, 44.4301]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_exporter.py -v`
Expected: FAIL (new columns absent from export; GeoJSON uses scraped coords).

- [ ] **Step 3: Update `_EXPORT_COLUMNS`**

In `src/storage/exporter.py`, add to `_EXPORT_COLUMNS` after `"cross_platform_group_id",`:

```python
    # Geo curation (precision + fused position)
    "operator_id", "property_group_id",
    "latitude_geocoded", "longitude_geocoded",
    "latitude_best", "longitude_best", "geocoded_address",
    "location_precision", "location_source", "est_accuracy_m", "position_confidence",
```

- [ ] **Step 4: Switch GeoJSON to best coords**

In `export_geojson`, replace the coordinate extraction:

```python
        props = dict(zip(_EXPORT_COLUMNS, row))
        lat = props.get("latitude_best") or props.pop("latitude")
        lng = props.get("longitude_best") or props.pop("longitude")
        props.pop("latitude", None)
        props.pop("longitude", None)
```

(Keeps `latitude_best`/`longitude_best` in `properties` too; geometry uses best when present, else scraped.)

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_exporter.py -v`
Expected: 2 passed.

- [ ] **Step 6: Add the historical-observation backfill reader to `database.py`**

Append to the `Database` class:

```python
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
```

- [ ] **Step 7: Commit**

```bash
git add src/storage/database.py src/storage/exporter.py tests/test_exporter.py
git commit -m "feat: export geo-curation columns, GeoJSON best coords, historical observation reader"
```

---

## Task 12: Orchestrator wiring + `--curate-only`

**Files:**
- Modify: `src/orchestrator.py:115-168` (replace `_link_cross_platform`), `:555-588` (CLI), `src/storage/exporter.py` (operator re-key + review exports)

- [ ] **Step 1: Re-key `get_operator_summary` on `operator_id`**

In `src/storage/database.py`, change the `get_operator_summary` SQL's key expression from
`COALESCE(business_registration_number, business_name, host_id)` to
`COALESCE(operator_id, business_registration_number, business_name, host_id)` (both the SELECT alias and the WHERE/GROUP BY). This makes the "who controls what" view use the real operator grouping.

- [ ] **Step 2: Replace `_link_cross_platform` with the curation stage**

In `src/orchestrator.py`, replace the `_link_cross_platform` method body with:

```python
    def _curate_geo_and_dedup(self):
        """Post-enrichment curation: operator linking, layered property dedup,
        geocoding + position fusion, and verification. Runs over the whole DB."""
        from .geo.curate import run_curation
        from .storage.database import Database

        backfill_rows = None
        backup = sorted(__import__("glob").glob(str(self.db.db_path) + ".backup-*"))
        if backup:
            try:
                backfill_rows = Database.read_historical_observations(backup[-1])
                logger.info("Curation: loaded %d historical observations from %s",
                            len(backfill_rows), backup[-1])
            except Exception as e:
                logger.warning("Curation: backfill skipped (%s)", e)

        metrics = run_curation(self.db, config=self.config, backfill_rows=backfill_rows)
        logger.info("Curation complete: %s", metrics)
```

Update the call site (Step 4 region, ~line 118) from `self._link_cross_platform()` to `self._curate_geo_and_dedup()`, keeping the surrounding try/except.

- [ ] **Step 3: Add `--curate-only` CLI path**

In `main()`, add a flag parse:

```python
        elif arg == "--curate-only":
            curate_only = True
```

Initialize `curate_only = False` near the other flags, and branch before `asyncio.run`:

```python
    if curate_only:
        try:
            orchestrator._curate_geo_and_dedup()
            from .storage.exporter import export_csv, export_geojson, export_operators_csv
            export_csv(orchestrator.db)
            export_geojson(orchestrator.db)
            export_operators_csv(orchestrator.db)
        finally:
            orchestrator.db.close()
        return
```

- [ ] **Step 4: Manual smoke (no network) on a copy**

Run:
```powershell
Copy-Item data/bucharest.db data/curate_smoke.db
python -c "from src.storage.database import Database; from src.geo.curate import run_curation; db=Database(db_path=__import__('pathlib').Path('data/curate_smoke.db')); print(run_curation(db, fetch_fn=lambda q: [])); db.close()"
```
Expected: prints a metrics dict; no exceptions. (`fetch_fn=lambda q: []` makes every geocode a 'not_found', so it exercises dedup+fusion without network.)

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.py src/storage/database.py
git commit -m "feat: wire geo/dedup curation into the pipeline + --curate-only entrypoint"
```

---

## Task 13: Review exports (sample CSV + metrics JSON + geo QA)

**Files:**
- Modify: `src/storage/exporter.py` (add `export_dedup_review`, `export_dedup_metrics`), `src/geo/curate.py` (write them)
- Test: `tests/test_exporter.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_exporter.py`:

```python
from src.storage.exporter import export_dedup_metrics


def test_export_dedup_metrics_writes_json(db, tmp_path):
    path = export_dedup_metrics({"precision_proxy": 0.97, "conflict_groups": []},
                                output_path=tmp_path / "m.json")
    import json
    assert json.load(open(path))["precision_proxy"] == 0.97
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_exporter.py::test_export_dedup_metrics_writes_json -v`
Expected: FAIL (function not defined).

- [ ] **Step 3: Implement exporters**

Append to `src/storage/exporter.py`:

```python
def export_dedup_metrics(metrics: dict, output_path: Path | None = None) -> Path:
    """Write the dedup verification metrics to JSON."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "dedup_metrics.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    logger.info("Exported dedup metrics to %s", path)
    return path


def export_dedup_review(db: Database, output_path: Path | None = None, sample: int = 200) -> Path:
    """Export a reviewable sample of property groups: members, names, coords,
    distance, identity keys, photo URLs — for eyeballing dedup quality."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "dedup_review.csv"
    rows = db.conn.execute(
        """SELECT property_group_id, id, platform, name, latitude, longitude,
                  latitude_best, longitude_best, business_registration_number,
                  business_phone, thumbnail_url
           FROM listings WHERE property_group_id IS NOT NULL
           ORDER BY property_group_id LIMIT ?""", (sample * 2,)
    ).fetchall()
    header = ["property_group_id", "id", "platform", "name", "latitude", "longitude",
              "latitude_best", "longitude_best", "registration", "phone", "thumbnail_url"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    logger.info("Exported dedup review sample to %s", path)
    return path
```

- [ ] **Step 4: Write them from `run_curation`**

At the end of `run_curation` (before `return metrics`), add:

```python
    try:
        from ..storage.exporter import export_dedup_metrics, export_dedup_review
        export_dedup_metrics(metrics)
        export_dedup_review(db)
    except Exception as e:
        logger.warning("Curation: review export failed (%s)", e)
```

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_exporter.py -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/exporter.py src/geo/curate.py tests/test_exporter.py
git commit -m "feat: dedup review CSV + metrics JSON exports"
```

---

## Task 14: Map popups show precision + best coords

**Files:**
- Modify: `src/visualization/map_builder.py`

- [ ] **Step 1: Read the current builder**

Run: open `src/visualization/map_builder.py` and locate the SELECT that feeds markers and the popup-HTML construction.

- [ ] **Step 2: Use best coords + add a precision line**

Change the marker coordinate source to `COALESCE(latitude_best, latitude)` / `COALESCE(longitude_best, longitude)` in the query, and append to the popup HTML (where business/host sections are built):

```python
        precision = props.get("location_precision") or "unverified"
        conf = props.get("position_confidence")
        source = props.get("location_source") or "platform_coord"
        popup_html += (
            f"<br><b>Location:</b> {precision}"
            + (f" (confidence {conf:.2f}, via {source})" if conf is not None else "")
        )
```

(Match the exact variable names already used in the file; this snippet assumes a `props` dict and a `popup_html` string — adapt to the file's existing pattern.)

- [ ] **Step 3: Smoke test the map build**

Run:
```powershell
python -c "from src.storage.database import Database; from src.visualization.map_builder import build_map; db=Database(db_path=__import__('pathlib').Path('data/curate_smoke.db')); print(build_map(db)); db.close()"
```
Expected: prints the output HTML path; file opens in a browser with location lines in popups.

- [ ] **Step 4: Commit**

```bash
git add src/visualization/map_builder.py
git commit -m "feat: map popups show location precision/confidence and plot best coords"
```

---

## Task 15: Full-suite green + self-review of behavior

**Files:** none (verification task)

- [ ] **Step 1: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: all tests pass.

- [ ] **Step 2: Confirm no regression in existing pipeline imports**

Run: `python -c "import src.orchestrator, src.storage.exporter, src.geo.curate, src.dedup.property_groups; print('imports ok')"`
Expected: `imports ok`.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "test: full suite green for geo/dedup curation" --allow-empty
```

---

## Task 16: Rollout — curate existing data, then rescrape

**Files:** none (operational task; run by the user / executor with confirmation)

- [ ] **Step 1: Back up the current DB**

Run:
```powershell
Copy-Item data/bucharest.db ("data/bucharest.db.backup-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
```

- [ ] **Step 2: Curate the existing data (geocodes ~4,700 Booking addresses, ~80 min, cached)**

Run: `python -m src.orchestrator --curate-only`
Expected: logs operator/group counts, geocode progress, fusion, and a metrics line; writes `data/exports/dedup_metrics.json`, `dedup_review.csv`, refreshed `listings.csv`/`.geojson`/`operators.csv`.

- [ ] **Step 2b: Review quality + tune**

Open `data/exports/dedup_metrics.json` — confirm `precision_proxy` ≥ ~0.95 and `conflict_groups` is small/explained. Eyeball `data/exports/dedup_review.csv`. If precision is low, raise `dedup.strict_name_threshold` / lower distances in `config/scraping.yaml` and re-run `--curate-only` (geocodes are cached, so it's fast).

- [ ] **Step 3: Back up the curated DB**

Run:
```powershell
Copy-Item data/bucharest.db ("data/bucharest.db.curated-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
```

- [ ] **Step 4: Full fresh rescrape (curation runs inline at the end)**

Run: `python -m src.orchestrator`
Expected: full scrape + enrichment + the curation stage; the ledger gains a third capture (temporal fusion tightens Airbnb positions further). Monitor `scraper.log`.

- [ ] **Step 5: Compare captures + update docs**

Extend `CAPTURE_COMPARISON.md` with a positions section (counts by `location_precision`, median `est_accuracy_m`, how many Airbnb listings improved via twin/temporal) and update `METHODOLOGY.md` §7/§8. Commit:

```bash
git add CAPTURE_COMPARISON.md METHODOLOGY.md
git commit -m "docs: report position-precision + dedup improvements post-curation"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** operator layer (T1–2), layered Tier-0/1/2 dedup (T4), precision tagging (T5), geocode+cache+retry (T7), observation ledger + temporal backfill (T3/T10/T11), inverse-variance fusion + disagreement handling (T6/T10), verification metrics + review exports (T8/T13), schema/exports/map/config/orchestrator (T3/T9/T11/T12/T14), rollout incl. rescrape (T16). All spec sections map to a task.
- **Placeholder scan:** no TBD/TODO; every code step has complete code. The map-builder task (T14) intentionally adapts to existing variable names — it includes the exact snippet and instructs matching the file's `props`/`popup_html` pattern, since that file wasn't read during planning.
- **Type consistency:** `assign_property_groups` returns the 3-tuple `(mapping, cross, identity_groups)` at every call site (T4 tests unpack `mapping, cross, _ident`; T10 unpacks all three and feeds `identity_groups` to `dedup_metrics`); `dedup_metrics(rows, mapping, excluded_groups)` signature matches its T8 test and the T10 call; `fuse_observations` returns `FusedPosition` with `dominant_listing_id`/`dominant_source` used consistently in T10; geocoder `fetch_fn` signature `(query)->list[dict]` consistent across T7/T10/T12; DB writer names (`set_operator_ids`, `set_property_groups`, `set_geocoded`, `set_fused_positions`, `add_position_observations`, `get_geocode`, `upsert_geocode`) consistent between T3 and T10.
