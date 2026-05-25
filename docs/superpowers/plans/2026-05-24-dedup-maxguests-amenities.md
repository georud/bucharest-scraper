# Same-Operator Over-Merge Fix (max_guests + amenities veto) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop the deduper merging distinct same-operator units by adding a same-platform "distinctness veto" (different `max_guests`, or low amenity-overlap) at the top of `_compatible`, gating both Tier-1 and Tier-2; plus capturing each Airbnb listing's amenity set via the existing PDP pass.

**Architecture:** The veto lives at the top of `_compatible(a, b, …)` in `src/dedup/property_groups.py` — the single chokepoint both tiers and the clique-growth check pass through. It only fires for **same-platform** pairs (different platform → never vetoed → cross-platform twins untouched). `max_guests` is already stored; amenities are captured by generalizing the radius PDP pass to grab radius **and** amenities in one fetch.

**Tech Stack:** Python 3.13, sqlite3 (JSON1), rapidfuzz, Playwright, pytest.

**Validation targets (after each re-curate):** the reported pair `airbnb_1584240306149342073` / `airbnb_1537649821496191361` get **different** `property_group_id`; cross-platform group count stays ≈ **1,495**; `recall_proxy` stays **1.0**.

---

### Task 1: Parser — `extract_amenities`

**Files:** Modify `src/scrapers/airbnb/parser.py` (add near `extract_map_radius`); Test `tests/test_airbnb_radius.py`.

- [ ] **Step 1: Write the failing test** (append to `tests/test_airbnb_radius.py`):

```python
from src.scrapers.airbnb.parser import extract_amenities

def test_extract_amenities():
    html = ('x{"__typename":"AmenityItem","available":true,"title":"Wifi","icon":"A"}'
            '{"__typename":"AmenityItem","available":true,"title":"Kitchen","icon":"B"}'
            '{"__typename":"AmenityItem","available":false,"title":"Pool","icon":"C"}y')
    assert extract_amenities(html) == ["kitchen", "wifi"]   # available only, normalized, sorted
    assert extract_amenities("no amenities here") == []
    assert extract_amenities(None) == []
```

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_airbnb_radius.py::test_extract_amenities -q` → FAIL (ImportError).

- [ ] **Step 3: Implement** (in `src/scrapers/airbnb/parser.py`, after `extract_map_radius`):

```python
_AMENITY_RE = re.compile(r'"__typename":"AmenityItem","available":(true|false),"title":"([^"]{1,80})"')


def extract_amenities(html: str | None) -> list[str]:
    """Sorted, de-duplicated, normalized (lowercase/stripped) titles of the
    AVAILABLE amenities on an Airbnb PDP. Returns [] if none/absent."""
    if not html:
        return []
    titles = {t.strip().lower() for (avail, t) in _AMENITY_RE.findall(html)
              if avail == "true" and t.strip()}
    return sorted(titles)
```

- [ ] **Step 4: Run to verify it passes.**
- [ ] **Step 5: Commit** — `git add src/scrapers/airbnb/parser.py tests/test_airbnb_radius.py && git commit -m "feat(airbnb): parse available amenities from PDP"`

---

### Task 2: DB — amenities column, writer, reader, curation cols

**Files:** Modify `src/storage/database.py`; Test `tests/test_database_curation.py`.

- [ ] **Step 1: Write failing tests** (append to `tests/test_database_curation.py`):

```python
def test_amenities_roundtrip_and_missing_pdp(db):
    _mk(db, "airbnb_a1", Platform.AIRBNB, "A", 44.43, 26.10, url="https://www.airbnb.com/rooms/a1")
    db.set_airbnb_location_radius({"airbnb_a1": 0.0})           # has radius, missing amenities
    assert "airbnb_a1" in {r["id"] for r in db.get_airbnb_listings_missing_pdp_details()}
    db.set_airbnb_amenities({"airbnb_a1": '["wifi","kitchen"]'})
    assert db.conn.execute("SELECT amenities FROM listings WHERE id='airbnb_a1'").fetchone()[0] == '["wifi","kitchen"]'
    assert "airbnb_a1" not in {r["id"] for r in db.get_airbnb_listings_missing_pdp_details()}  # now complete

def test_curation_cols_include_maxguests_amenities(db):
    _mk(db, "airbnb_a2", Platform.AIRBNB, "A", 44.43, 26.10, max_guests=8)
    db.set_airbnb_amenities({"airbnb_a2": '["tv"]'})
    row = next(r for r in db.get_listings_for_curation() if r["id"] == "airbnb_a2")
    assert row["max_guests"] == 8 and row["amenities"] == '["tv"]'
```

- [ ] **Step 2: Run to verify they fail** (no `amenities` column / methods).

- [ ] **Step 3: Implement** in `src/storage/database.py`:
  - In `_migrate`'s `new_columns` list, after `("airbnb_location_radius_m", "REAL"),` add `("amenities", "TEXT"),`.
  - Add `"max_guests", "amenities"` to the `_CURATION_COLS` tuple (after `"airbnb_location_radius_m"`).
  - Add the writer + rename the reader:

```python
    def set_airbnb_amenities(self, mapping: dict[str, str]) -> int:
        """mapping: {listing_id: amenities_json}. JSON list of normalized titles."""
        if not mapping:
            return 0
        self.conn.executemany("UPDATE listings SET amenities=? WHERE id=?",
                              [(j, lid) for lid, j in mapping.items()])
        self.conn.commit()
        return len(mapping)

    def get_airbnb_listings_missing_pdp_details(self, limit: int | None = None) -> list[dict]:
        """Airbnb listings missing radius OR amenities (need a PDP fetch), as
        lightweight {id, platform_id, url} records."""
        sql = ("SELECT id, platform_id, url FROM listings WHERE platform='airbnb' "
               "AND url IS NOT NULL AND (airbnb_location_radius_m IS NULL OR amenities IS NULL)")
        params: tuple = ()
        if limit is not None:
            sql += " LIMIT ?"; params = (limit,)
        return [{"id": r[0], "platform_id": r[1], "url": r[2]}
                for r in self.conn.execute(sql, params).fetchall()]
```
  Delete the old `get_airbnb_listings_missing_radius` (Task 3 updates its only caller). `reset_curation_columns` does **not** touch `amenities` (scraper data, like the radius).

- [ ] **Step 4: Run to verify they pass.**
- [ ] **Step 5: Commit** — `git add src/storage/database.py tests/test_database_curation.py && git commit -m "feat(db): amenities column + writer + missing-pdp-details reader; max_guests/amenities in curation cols"`

---

### Task 3: Scraper — capture radius + amenities in one PDP pass

**Files:** Modify `src/scrapers/airbnb/scraper.py`, `src/orchestrator.py`.

- [ ] **Step 1:** In `src/scrapers/airbnb/scraper.py`, update the import to add `extract_amenities`, then generalize the fetch + capture methods. Replace `_fetch_map_radius` with `_fetch_pdp_details` returning both, and rename `capture_location_radius` → `capture_pdp_details` (same batched-browser/checkpoint structure):

```python
    async def _fetch_pdp_details(self, context, url: str, timeout_ms: int) -> dict:
        """Load a listing PDP, return {radius, amenities}."""
        page = await context.new_page()
        try:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception as e:
                logger.debug("Navigation failed for %s: %s", url, e)
                return {}
            try:
                await page.wait_for_function(
                    "() => document.documentElement.innerHTML.indexOf('mapMarkerRadiusInMeters') !== -1",
                    timeout=12_000)
            except Exception:
                pass
            html = await page.content()
            return {"radius": extract_map_radius(html), "amenities": extract_amenities(html)}
        finally:
            try:
                await page.close()
            except Exception:
                pass
```
  In `capture_pdp_details` (the renamed batched loop), per listing call `_fetch_pdp_details`; accumulate `radii[id]=d["radius"]` when not None and `amen[id]=json.dumps(d["amenities"])` when `d.get("amenities")`; per-batch checkpoint via `db.set_airbnb_location_radius(batch_radii)` **and** `db.set_airbnb_amenities(batch_amen)`. (Add `import json` at top if absent.) Keep the tqdm/`BATCH_SIZE`/fresh-browser pattern unchanged.

- [ ] **Step 2:** In `src/orchestrator.py`, rename `capture_airbnb_radius` → `capture_airbnb_pdp_details` calling `scraper.capture_pdp_details(self.db.get_airbnb_listings_missing_pdp_details(limit), db=self.db)`; keep the `--capture-airbnb-radius` flag but point it at the renamed method (the flag now captures radius **and** amenities). Update the log line text.

- [ ] **Step 3: Verify nothing else references the old names** — `grep -rn "capture_location_radius\|_fetch_map_radius\|get_airbnb_listings_missing_radius" src/` → expect **no hits**.

- [ ] **Step 4: Run** `python -m pytest tests/ -q` → all pass (no behavioral test here; this is the scrape pass, exercised by the rollout). Then **commit** — `git add src/scrapers/airbnb/scraper.py src/orchestrator.py && git commit -m "feat(airbnb): capture radius + amenities in one PDP pass (capture_pdp_details)"`

---

### Task 4: The same-platform distinctness veto + exporter

**Files:** Modify `src/dedup/property_groups.py`, `src/storage/exporter.py`; Test `tests/test_property_groups.py`.

- [ ] **Step 1: Write failing tests** (append to `tests/test_property_groups.py` — uses `_compatible`; import it). Build row dicts with `platform`, coords, `name`, `bedrooms/beds/bathrooms`, `max_guests`, `amenities`:

```python
from src.dedup.property_groups import _compatible

def _r(pid, plat="airbnb", lat=44.4300, lng=26.1000, name="Flat", mg=None, am=None,
       bed=4, beds=6, bath=2.5):
    return {"id": pid, "platform": plat, "latitude": lat, "longitude": lng, "name": name,
            "bedrooms": bed, "beds": beds, "bathrooms": bath, "max_guests": mg, "amenities": am}

def test_veto_max_guests_blocks_both_tiers():
    a = _r("a", mg=12); b = _r("b", lat=44.4309, name="Flat", mg=14)   # ~100 m, same room, diff capacity
    assert _compatible(a, b, relaxed=True) is False     # Tier-1 (operator block) vetoed
    a2 = _r("a", name="Cozy Central Studio", mg=12)
    b2 = _r("b", lat=44.4305, name="Cozy Central Studio", mg=14)        # name ~100, <100 m -> Tier-2 candidate
    assert _compatible(a2, b2, relaxed=False) is False  # Tier-2 vetoed too

def test_veto_amenities_low_jaccard():
    a = _r("a", mg=8, am='["wifi","kitchen","tv","ac"]')
    b = _r("b", lat=44.4305, mg=8, am='["pool","gym","sauna","parking"]')  # same capacity, disjoint amenities
    assert _compatible(a, b, relaxed=True) is False

def test_veto_noop_cross_platform_and_missing():
    # Different platform -> never vetoed (Booking has no max_guests/amenities)
    a = _r("a", plat="booking", mg=None); b = _r("b", plat="airbnb", lat=44.4305, mg=14)
    assert _compatible(a, b, relaxed=True) is True      # falls through to room/dist match -> compatible
    # Same platform but capacity unknown + amenities absent -> no veto, still compatible
    c = _r("c", mg=None, am=None); d = _r("d", lat=44.4305, mg=None, am=None)
    assert _compatible(c, d, relaxed=True) is True
```

- [ ] **Step 2: Run to verify they fail** (no veto yet → `test_veto_*` return True).

- [ ] **Step 3: Implement** in `src/dedup/property_groups.py`:

```python
import json

AMENITY_JACCARD_MIN = 0.6


def _amenity_set(row: dict) -> set[str]:
    raw = row.get("amenities")
    if not raw:
        return set()
    try:
        return set(json.loads(raw))
    except (ValueError, TypeError):
        return set()


def _jaccard(s1: set, s2: set) -> float:
    if not s1 or not s2:
        return 1.0           # unknown -> don't discriminate
    return len(s1 & s2) / len(s1 | s2)


def _same_platform_distinct(a: dict, b: dict, amenity_jaccard_min: float = AMENITY_JACCARD_MIN) -> bool:
    """True if a and b are the SAME platform but demonstrably different units.
    Cross-platform pairs are never distinct here (Booking exposes neither signal)."""
    if a["platform"] != b["platform"]:
        return False
    amg, bmg = a.get("max_guests"), b.get("max_guests")
    if amg is not None and bmg is not None and amg != bmg:
        return True
    sa, sb = _amenity_set(a), _amenity_set(b)
    if sa and sb and _jaccard(sa, sb) < amenity_jaccard_min:
        return True
    return False
```
  Then add `amenity_jaccard_min: float = AMENITY_JACCARD_MIN` to `_compatible`'s signature and make it the **first** check in the body:

```python
    if _same_platform_distinct(a, b, amenity_jaccard_min):
        return False
```
  Thread it through `assign_property_groups`: read `amenity_jaccard_min = getattr(dedup_cfg, "amenity_jaccard_min", AMENITY_JACCARD_MIN)` and pass it in the local `compat(x, y, relaxed)` wrapper (which calls `_compatible(...)`).

- [ ] **Step 4:** In `src/storage/exporter.py`, add `"amenities"` to `_EXPORT_COLUMNS` (in the geo/curation group, near `airbnb_location_radius_m`).

- [ ] **Step 5: Run** `python -m pytest tests/ -q` → all pass.
- [ ] **Step 6: Commit** — `git add src/dedup/property_groups.py src/storage/exporter.py tests/test_property_groups.py && git commit -m "feat(dedup): same-platform distinctness veto (max_guests + amenity Jaccard) gating both tiers"`

---

### Task 5: Rollout — re-curate, validate, then the amenity scrape

**Files:** none (operational). Each `--curate-only`/capture run regenerates exports + map.

- [ ] **Step 1: Ship the max_guests veto (no scrape).** Back up, then re-curate:
  `cp data/bucharest.db "data/bucharest.db.prededup-$(date +%Y%m%d-%H%M%S)" && python -m src.orchestrator --curate-only`
- [ ] **Step 2: Validate the max_guests fix.** Confirm: the two reported ids now have **different** `property_group_id`; `SELECT COUNT(DISTINCT cross_platform_group_id)` ≈ 1,495; `dedup_metrics.json` `recall_proxy` == 1.0. If recall dropped or cross-platform groups fell sharply → STOP and investigate (the veto must not touch cross-platform).
- [ ] **Step 3: Run the amenity capture** (multi-hour, monitored): `python -m src.orchestrator --capture-airbnb-radius` (now also fetches amenities for the ~6,185 Airbnb via `get_airbnb_listings_missing_pdp_details`). It re-curates + re-exports at the end.
- [ ] **Step 4: Validate amenity veto + record final numbers.** `SELECT COUNT(*) FROM listings WHERE amenities IS NOT NULL` (~most Airbnb); re-confirm recall 1.0 + cross-platform ≈ 1,495; spot-check a handful of newly-split within-Airbnb groups are genuinely distinct units. Report the drop in within-Airbnb over-merge groups.

---

## Self-review

- **Spec coverage:** veto in `_compatible` covering both tiers (T4) ✓; max_guests clause (T4) + max_guests in `_CURATION_COLS` (T2) ✓; amenities parse (T1) + capture (T3) + column/writer/reader/curation-cols (T2) + Jaccard veto clause (T4) ✓; cross-platform/missing no-op (T4 tests) ✓; exporter (T4) ✓; rollout max_guests-first then scrape (T5) ✓; validation targets stated ✓.
- **Placeholder scan:** none — every step has real code/commands.
- **Type consistency:** `extract_amenities`→list[str]; stored as `json.dumps(list)`; `_amenity_set` does `json.loads`→set; `get_airbnb_listings_missing_pdp_details` returns `{id,platform_id,url}` dicts (matches the scraper's record use); `_compatible` gains `amenity_jaccard_min` and `assign_property_groups` threads it via `getattr(dedup_cfg, "amenity_jaccard_min", …)`.
