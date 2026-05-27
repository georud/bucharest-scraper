# Cross-platform offset + σ calibration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Persist the per-twin Airbnb↔Booking positional offset as exported columns and roll it up into a per-run `position_calibration` block that corroborates the σ priors and soft-warns on drift.

**Architecture:** A new pure module `src/geo/calibration.py` holds two functions — `compute_offsets` (per-cross-platform-group pairwise distances → per-group median offset + geocoded calibration pairs) and `sigma_calibration` (buckets geocoded pairs by the Airbnb point's assigned σ, compares measured median to `√(σ² + σ_geocoded²)`). `curate.py` captures the σ already assigned in its observation loop, calls both, persists the offset via a new DB writer, and adds the calibration to the existing `metrics` dict before export. Two new curation-derived columns flow to the CSV/geojson.

**Tech Stack:** Python 3.13, sqlite3, pytest, stdlib `statistics`/`math`.

**Spec:** `docs/superpowers/specs/2026-05-27-cross-platform-offset-and-sigma-calibration-design.md` (`799419f`). Work on `master`. Don't break existing curation/tests.

---

### Task 1: `sigma_calibration` in `src/geo/calibration.py`

**Files:**
- Create: `src/geo/calibration.py`
- Test: `tests/test_calibration.py`

- [ ] **Step 1: Write the failing test** — create `tests/test_calibration.py`:

```python
import math
from src.geo.calibration import sigma_calibration, WARN_BAND


def _pairs(sigma, dists):
    return [{"distance_m": d, "airbnb_sigma": sigma} for d in dists]


def test_distinct_sigma_distinct_buckets_and_predicted():
    recs = _pairs(106.4, [99, 99, 99]) + _pairs(350.0, [300, 300])
    out = sigma_calibration(recs, geo_sigma=25.0, max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=2)
    sig = {b["airbnb_sigma"]: b for b in out["buckets"]}
    assert set(sig) == {106.4, 350.0}                       # one bucket per σ, not lumped
    assert sig[106.4]["n"] == 3 and sig[350.0]["n"] == 2
    assert sig[106.4]["predicted_m"] == round(math.hypot(106.4, 25.0), 1)
    assert sig[106.4]["measured_median_m"] == 99.0
    assert sig[106.4]["ratio"] == round(99.0 / math.hypot(106.4, 25.0), 2)


def test_max_dist_excludes_outliers():
    out = sigma_calibration(_pairs(100.0, [100, 100, 5000]), geo_sigma=25.0,
                            max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=1)
    assert out["buckets"][0]["n"] == 2                      # the 5000 m pair dropped
    assert out["buckets"][0]["measured_median_m"] == 100.0


def test_out_of_band_warns_only_when_enough_n():
    big = sigma_calibration(_pairs(100.0, [400] * 30), geo_sigma=25.0,
                            max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=30)
    assert big["buckets"][0]["ratio"] > 1.4 and big["buckets"][0]["warned"] is True
    small = sigma_calibration(_pairs(100.0, [400] * 5), geo_sigma=25.0,
                              max_dist_m=1000.0, warn_band=(0.6, 1.4), min_n=30)
    assert small["buckets"][0]["ratio"] > 1.4 and small["buckets"][0]["warned"] is False


def test_empty_input():
    out = sigma_calibration([], geo_sigma=25.0, max_dist_m=1000.0, warn_band=(0.6, 1.4))
    assert out["buckets"] == [] and out["warn_band"] == [0.6, 1.4]


def test_default_warn_band_constant():
    assert WARN_BAND == (0.6, 1.4)
```

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_calibration.py -q` → FAIL (ImportError: cannot import name `sigma_calibration`).

- [ ] **Step 3: Implement** — create `src/geo/calibration.py`:

```python
from __future__ import annotations

import math
from collections import defaultdict
from statistics import median

# Default acceptance band for the measured-median / predicted-RMS ratio.
# A correctly-calibrated ladder yields ~0.9 (a median compared to an RMS), so the
# band is centred on that, not on 1.0. Below lo -> priors too pessimistic; above
# hi -> measured error exceeds even the RMS prediction (priors too optimistic).
WARN_BAND = (0.6, 1.4)


def sigma_calibration(pair_records, *, geo_sigma, max_dist_m, warn_band=WARN_BAND, min_n=30):
    """Bucket geocoded-Booking cross-platform pairs by the Airbnb point's assigned
    σ and compare the measured median displacement to the predicted RMS.

    pair_records: iterable of {"distance_m": float, "airbnb_sigma": float}.
    Returns a JSON-serialisable dict:
      {"warn_band": [lo, hi], "min_n": int, "buckets": [
         {"airbnb_sigma", "n", "measured_median_m", "predicted_m", "ratio", "warned"}...]}
    Buckets are sorted by airbnb_sigma. `warned` is True only when n >= min_n AND
    the ratio is outside warn_band (small buckets are reported, never warned)."""
    lo, hi = warn_band
    by_sigma: dict[float, list[float]] = defaultdict(list)
    for r in pair_records:
        s = r.get("airbnb_sigma")
        d = r.get("distance_m")
        if s is None or d is None or d > max_dist_m:
            continue
        by_sigma[round(float(s), 1)].append(float(d))

    buckets = []
    for sigma in sorted(by_sigma):
        dists = by_sigma[sigma]
        predicted = math.hypot(sigma, geo_sigma)
        measured = float(median(dists))
        ratio = measured / predicted if predicted else 0.0
        warned = len(dists) >= min_n and not (lo <= ratio <= hi)
        buckets.append({
            "airbnb_sigma": sigma,
            "n": len(dists),
            "measured_median_m": round(measured, 1),
            "predicted_m": round(predicted, 1),
            "ratio": round(ratio, 2),
            "warned": warned,
        })
    return {"warn_band": [lo, hi], "min_n": min_n, "buckets": buckets}
```

- [ ] **Step 4: Run to verify it passes** — `python -m pytest tests/test_calibration.py -q` → PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/geo/calibration.py tests/test_calibration.py
git commit -m "feat(geo): sigma_calibration — bucket cross-platform pairs by assigned sigma vs predicted RMS"
```

---

### Task 2: `compute_offsets` in `src/geo/calibration.py`

**Files:**
- Modify: `src/geo/calibration.py`
- Test: `tests/test_calibration.py`

- [ ] **Step 1: Write the failing test** — append to `tests/test_calibration.py`:

```python
from src.geo.calibration import compute_offsets


def _row(lid, platform, lat, lng):
    return {"id": lid, "platform": platform, "latitude": lat, "longitude": lng}


def test_compute_offsets_geocoded_pair():
    by_id = {"ab": _row("ab", "airbnb", 44.4300, 26.1000),
             "bk": _row("bk", "booking", 44.4300, 26.1000)}
    members_by_key = {"g1": ["ab", "bk"]}
    geocoded_map = {"bk": (44.4309, 26.1000, "addr")}   # ~100 m north of the pin
    scraped_sigma = {"ab": 106.4, "bk": 50.0}
    offsets, calib = compute_offsets({"g1"}, members_by_key, by_id, geocoded_map, scraped_sigma)
    # both members carry the same offset; source is geocoded
    assert offsets["ab"][1] == "geocoded" and offsets["bk"][1] == "geocoded"
    assert 95 <= offsets["ab"][0] <= 105 and offsets["ab"][0] == offsets["bk"][0]
    # calibration pair carries the Airbnb point's sigma + the geocoded distance
    assert len(calib) == 1 and calib[0]["airbnb_sigma"] == 106.4
    assert abs(calib[0]["distance_m"] - offsets["ab"][0]) < 1.0


def test_compute_offsets_scraped_fallback_and_no_calib():
    by_id = {"ab": _row("ab", "airbnb", 44.4300, 26.1000),
             "bk": _row("bk", "booking", 44.4305, 26.1000)}
    members_by_key = {"g1": ["ab", "bk"]}
    offsets, calib = compute_offsets({"g1"}, members_by_key, by_id, {}, {"ab": 15.0})
    assert offsets["ab"][1] == "scraped"
    assert calib == []                                  # no geocoded pair -> no calibration input


def test_compute_offsets_skips_single_platform_group():
    by_id = {"ab": _row("ab", "airbnb", 44.43, 26.10), "ab2": _row("ab2", "airbnb", 44.43, 26.10)}
    offsets, calib = compute_offsets({"g1"}, {"g1": ["ab", "ab2"]}, by_id, {}, {"ab": 15.0, "ab2": 15.0})
    assert offsets == {} and calib == []
```

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_calibration.py -k compute_offsets -q` → FAIL (ImportError: `compute_offsets`).

- [ ] **Step 3: Implement** — add to `src/geo/calibration.py` (add the import at the top with the others):

```python
from ..dedup.deduplicator import haversine_distance
```

```python
def compute_offsets(cross_groups, members_by_key, by_id, geocoded_map, scraped_sigma):
    """For each cross-platform group, the Airbnb-pin <-> Booking-(geocoded-else-scraped)
    distances. Returns (offset_writes, calib_pairs):
      offset_writes: {listing_id: (offset_m, source)} — the group's median distance +
        "geocoded"/"scraped", written to EVERY member of the group.
      calib_pairs: [{"distance_m", "airbnb_sigma"}] — one per geocoded-Booking pair,
        the calibration input."""
    offset_writes: dict[str, tuple[float, str]] = {}
    calib_pairs: list[dict] = []
    for gid in cross_groups:
        members = members_by_key.get(gid, [])
        ab = [m for m in members if by_id[m]["platform"] == "airbnb"]
        bk = [m for m in members if by_id[m]["platform"] == "booking"]
        if not ab or not bk:
            continue
        dists: list[float] = []
        used_geocoded = False
        for a in ab:
            ar = by_id[a]
            a_sigma = scraped_sigma.get(a)
            for b in bk:
                if b in geocoded_map:
                    blat, blng, _ = geocoded_map[b]
                    is_geo = True
                else:
                    br = by_id[b]
                    blat, blng = br["latitude"], br["longitude"]
                    is_geo = False
                d = haversine_distance(ar["latitude"], ar["longitude"], blat, blng)
                dists.append(d)
                if is_geo:
                    used_geocoded = True
                    if a_sigma is not None:
                        calib_pairs.append({"distance_m": d, "airbnb_sigma": a_sigma})
        if not dists:
            continue
        value = (round(float(median(dists)), 1), "geocoded" if used_geocoded else "scraped")
        for m in members:
            offset_writes[m] = value
    return offset_writes, calib_pairs
```

- [ ] **Step 4: Run to verify it passes** — `python -m pytest tests/test_calibration.py -q` → PASS (8 tests total).

- [ ] **Step 5: Commit**

```bash
git add src/geo/calibration.py tests/test_calibration.py
git commit -m "feat(geo): compute_offsets — per-twin cross-platform offset + calibration pairs"
```

---

### Task 3: DB column + writer + reset

**Files:**
- Modify: `src/storage/database.py`
- Test: `tests/test_database_curation.py`

- [ ] **Step 1: Write the failing tests** — append to `tests/test_database_curation.py`:

```python
def test_cross_platform_offset_roundtrip_and_reset(db):
    _mk(db, "airbnb_off1", Platform.AIRBNB, "A", 44.43, 26.10)
    db.set_cross_platform_offsets({"airbnb_off1": (123.4, "geocoded")})
    row = db.conn.execute(
        "SELECT cross_platform_offset_m, cross_platform_offset_source FROM listings WHERE id='airbnb_off1'"
    ).fetchone()
    assert row[0] == 123.4 and row[1] == "geocoded"
    db.reset_curation_columns()
    row = db.conn.execute(
        "SELECT cross_platform_offset_m, cross_platform_offset_source FROM listings WHERE id='airbnb_off1'"
    ).fetchone()
    assert row[0] is None and row[1] is None       # curation-derived -> cleared
```

Also add `"cross_platform_offset_m"` and `"cross_platform_offset_source"` to the column list asserted in the existing `test_new_columns_present`.

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_database_curation.py -k "cross_platform_offset or new_columns" -q` → FAIL (no such column / no `set_cross_platform_offsets`).

- [ ] **Step 3: Implement** in `src/storage/database.py`:
  - In `_migrate`'s `new_columns` list, after `("amenities", "TEXT"),` add:
    ```python
            ("cross_platform_offset_m", "REAL"),
            ("cross_platform_offset_source", "TEXT"),
    ```
  - In `reset_curation_columns`, add to the `UPDATE listings SET …` column list (alongside `platform_precision=NULL`):
    ```python
                 cross_platform_offset_m=NULL, cross_platform_offset_source=NULL
    ```
  - Add the writer (model it on `set_airbnb_amenities`):
    ```python
    def set_cross_platform_offsets(self, mapping: dict[str, tuple[float, str]]) -> int:
        """mapping: {listing_id: (offset_m, source)}. Cross-platform twin disagreement,
        curation-derived; written to every member of a cross-platform group."""
        if not mapping:
            return 0
        self.conn.executemany(
            "UPDATE listings SET cross_platform_offset_m=?, cross_platform_offset_source=? WHERE id=?",
            [(off, src, lid) for lid, (off, src) in mapping.items()],
        )
        self.conn.commit()
        return len(mapping)
    ```

- [ ] **Step 4: Run to verify it passes** — `python -m pytest tests/test_database_curation.py -q` → PASS. Then `python -m pytest tests/ -q` → all green.

- [ ] **Step 5: Commit**

```bash
git add src/storage/database.py tests/test_database_curation.py
git commit -m "feat(db): cross_platform_offset_m/_source column + writer (curation-derived)"
```

---

### Task 4: Wire into `curate.py`

**Files:**
- Modify: `src/geo/curate.py`

- [ ] **Step 1: Capture the assigned σ in the observation loop.** Near the top of `run_curation` where the other accumulators are declared (next to `geocoded_map: dict[str, tuple] = {}`), add:

```python
    scraped_sigma: dict[str, float] = {}
```

In the `for r in rows:` loop, the existing call is:

```python
        _, sigma = classify_scraped_precision(
            r, stack[(round(r["latitude"], 6), round(r["longitude"], 6))], sigmas=fusion_cfg)
```

Immediately after it (still inside the loop, before the observation is appended), add:

```python
        scraped_sigma[lid] = sigma
```

- [ ] **Step 2: Compute + persist offsets and build the calibration input.** After `members_by_key` is fully built (the block that ends `members_by_key[group_key(r["id"])].append(r["id"])`), and before the fusion loop, add:

```python
    from .calibration import compute_offsets, sigma_calibration, WARN_BAND
    offset_writes, calib_pairs = compute_offsets(
        cross_groups, members_by_key, by_id, geocoded_map, scraped_sigma)
    db.set_cross_platform_offsets(offset_writes)
```

- [ ] **Step 3: Add the calibration to `metrics` + soft-warn.** Right after `metrics["geo_conflict_groups"] = geo_conflicts` (and before `logger.info("Curation metrics: %s", metrics)` / the export), add:

```python
    calib = sigma_calibration(
        calib_pairs,
        geo_sigma=geo_sigma,
        max_dist_m=disagreement_m,
        warn_band=getattr(fusion_cfg, "calibration_warn_band", WARN_BAND),
    )
    metrics["position_calibration"] = calib
    for b in calib["buckets"]:
        if b["warned"]:
            logger.warning(
                "σ calibration drift: σ=%.0f n=%d measured_median=%.0fm predicted=%.0fm ratio=%.2f (band %s)",
                b["airbnb_sigma"], b["n"], b["measured_median_m"], b["predicted_m"], b["ratio"], calib["warn_band"])
```

(`geo_sigma` is defined at `curate.py:50`; `disagreement_m` at `:120`; `cross_groups` from `assign_property_groups` at `:42`. The import is local to avoid any circular-import risk.)

- [ ] **Step 4: Run the suite** — `python -m pytest tests/ -q` → all green (no behavioural test added here; covered by Tasks 1–2 unit tests + the rollout). Confirm no import errors: `python -c "import src.geo.curate"`.

- [ ] **Step 5: Commit**

```bash
git add src/geo/curate.py
git commit -m "feat(curate): persist cross-platform offsets + emit position_calibration"
```

---

### Task 5: Export the two columns

**Files:**
- Modify: `src/storage/exporter.py`

- [ ] **Step 1: Add the columns.** In `_EXPORT_COLUMNS`, in the geo/curation group near `"airbnb_location_radius_m"` / `"amenities"`, add `"cross_platform_offset_m"` and `"cross_platform_offset_source"` as plain string entries (they are NOT derived columns — do not touch `_DERIVED_AFTER`).

- [ ] **Step 2: Verify** — `python -m pytest tests/test_exporter.py -q` → PASS (the export-column tests still pass; the module-level `_anchor in _EXPORT_COLUMNS` assertion is unaffected).

- [ ] **Step 3: Commit**

```bash
git add src/storage/exporter.py
git commit -m "feat(export): add cross_platform_offset_m/_source to listings export"
```

---

### Task 6: Document in `METHODOLOGY.md`

**Files:**
- Modify: `METHODOLOGY.md`

- [ ] **Step 1: §8 "Position hierarchy" (the σ-ladder block, ~line 414–447).** After the existing paragraph about the σ floor / "Booking exposes no per-listing…", add:

```markdown
**The σ ladder is now corroborated each run.** Curation emits a
`position_calibration` block in `dedup_metrics.json`: for cross-platform twins it
measures the Airbnb-pin↔Booking-*geocoded* displacement, buckets it by the σ the
ladder assigned the Airbnb point, and compares the measured median to the
predicted RMS `√(σ² + σ_geocoded²)`. The **exact-pin bucket cross-checks the
geocoder** (σ ≈ 25 m, measured ~26 m) and the **fuzz bucket the Airbnb obfuscation**
(σ ≈ 106 m for the standard 152 m circle, measured ~99 m). Both land at
`ratio ≈ 0.9` — the expected value for a median compared to an RMS, i.e. **in-band**.
So the Airbnb σ, originally a modelling assumption, is empirically confirmed
against geocoded twins; a bucket drifting outside the band logs a warning to
recheck the ladder.
```

- [ ] **Step 2: §11 Data dictionary (the column table, ~line 533).** Add two rows in the same `| column | description | NULL meaning |` format used by the surrounding rows (e.g. next to `cross_platform_group_id`):

```markdown
| `cross_platform_offset_m` | Distance (m) between the Airbnb pin and the Booking geocoded address for a twinned property (median over pairs); on every member of a cross-platform group (§8) | not a cross-platform twin |
| `cross_platform_offset_source` | Which Booking anchor the offset used: `geocoded` (street address) or `scraped` (map coord) | not a cross-platform twin |
```

- [ ] **Step 3: Commit** (METHODOLOGY is tracked — reports stay untracked)

```bash
git add METHODOLOGY.md
git commit -m "docs(methodology): document cross-platform offset columns + sigma calibration"
```

---

### Rollout (operational — run interactively with the user, NOT a subagent)

- [ ] **Step 1: Back up + re-curate.** `cp data/bucharest.db "data/bucharest.db.precalib-$(date +%Y%m%d-%H%M%S)"` then `python -m src.orchestrator --curate-only` (cache-fast geocode; migration adds the columns; regenerates exports + `dedup_metrics.json`).
- [ ] **Step 2: Validate.** Confirm: `cross_platform_offset_m` populates for cross-platform twins and is `NULL` for non-twins (`SELECT COUNT(*) FROM listings WHERE cross_platform_offset_m IS NOT NULL`); `dedup_metrics.json` `position_calibration` shows the exact-pin (σ 15) bucket `ratio ≈ 0.9` and the σ-106 bucket `ratio ≈ 0.9`, both `warned: false`; no σ-calibration drift warning in the log. Spot-check a known twin's offset against the §1 analysis (~26 m exact-pin, ~99 m fuzzed). Confirm `recall_proxy` still 1.0 (curation otherwise unchanged).
- [ ] **Step 3: Commit METHODOLOGY** (already committed in Task 6; if the validated numbers differ, update §8 text first). Reports refresh locally only. Push on the user's say-so.

---

## Self-review

- **Spec coverage:** Shared core → Task 2 (`compute_offsets`) ✓; column + writer + reset → Task 3 ✓; export → Task 5 ✓; `sigma_calibration` (bucket by σ, predicted RMS, min_n, max_dist, WARN_BAND) → Task 1 ✓; curate wiring (scraped_sigma capture, compute_offsets, set writer, metrics + soft-warn before export) → Task 4 ✓; §8 + §11 docs → Task 6 ✓; rollout + validation → Rollout ✓.
- **Placeholder scan:** none — every step has real code/commands.
- **Type consistency:** `compute_offsets` returns `(offset_writes: {id:(float,str)}, calib_pairs: [{distance_m,airbnb_sigma}])`; `set_cross_platform_offsets` consumes `{id:(offset_m,source)}` (matches); `sigma_calibration(pair_records, *, geo_sigma, max_dist_m, warn_band, min_n)` consumes `calib_pairs` (matches) and returns `{warn_band,min_n,buckets:[{airbnb_sigma,n,measured_median_m,predicted_m,ratio,warned}]}`; curate reads `b["warned"]`/`b["airbnb_sigma"]`/… (matches). `WARN_BAND=(0.6,1.4)` defined in Task 1, imported in Task 4. `predicted_m = math.hypot(sigma, geo_sigma)` = `√(σ²+geo²)` ✓.
