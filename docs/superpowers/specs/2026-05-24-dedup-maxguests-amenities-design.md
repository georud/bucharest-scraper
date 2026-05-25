# Reduce same-operator over-merge: max_guests + amenities — design

*2026-05-24*

## Context

The deduper over-merges **distinct properties of the same operator**. Reported
case: two Elvora listings — `airbnb_1584240306149342073` ("…Sleeps 12…") and
`airbnb_1537649821496191361` ("…Sleeps 14…") — 96 m apart, same operator
(reg `52410844`), **identical room config** (4 bed / 6 beds / 2.5 bath), got the
same `property_group_id`. They merged via **Tier 1** (operator block: ≤ 250 m AND
(name ≥ 70 OR room-config match)) on the room match, which ignores that they
sleep 12 vs 14 — different units.

The over-merge lives entirely in **same-platform (within-Airbnb) groups**: 415
such multi-member groups, **190 with members of differing `max_guests`**. The
1,495 cross-platform (Booking↔Airbnb) twin groups are the *legitimate* Tier-1
target and must not be touched (`recall_proxy = 1.0`).

Goal (user chose "both"): add two **same-platform-only** discriminators — the
free `max_guests` we already store, and a captured **amenity set** — without
weakening cross-platform twin detection.

## Findings (investigation)

- The two listings differ in `max_guests` (12 vs 14) — already stored, unused in
  the room-config match.
- Amenities are **not** in the stored search payload (~5.5 KB); they live only on
  the PDP, as `AmenityItem` objects:
  `{"__typename":"AmenityItem","available":true,"title":"Wifi","icon":"SYSTEM_WI_FI"}`
  (L1 exposed 64 items). Reliably extractable; **store the available-amenity
  *titles*** (normalized lowercase/trim — icon ids could be restructured; titles
  are readable and, with a pinned `en-US` locale + normalization, stable enough).

## Design

### The fix — a same-platform "distinctness veto" in `_compatible`

The over-merge spans **both** tiers — Tier-1's room-only path *and* Tier-2's pure
`dist ≤ 100 m AND name ≥ 80` (no content check). A planning check confirmed it:
`max_guests` placed only in `room_config_matches` would fix just ~100 of the 190
differing-capacity groups; the other **~90 leak through Tier-2**. So the
discriminator must sit where **both tiers and the clique-growth check** pass
through — the top of `_compatible(a, b, …)` (`src/dedup/property_groups.py`).
For **same-platform** pairs only, veto the match when the units are demonstrably
different:

```
if a["platform"] == b["platform"]:        # cross-platform pairs skip the veto
    if max_guests both present and differ:               -> return False
    if amenities both present and jaccard(a, b) < 0.6:   -> return False
# else fall through to the existing relaxed (Tier-1) / strict (Tier-2) logic
```

- **`max_guests` veto** — free (already stored); fixes the reported case + ~100 of
  the 190 differing-capacity groups outright. A genuine duplicate shares capacity,
  so it never splits a true dupe.
- **amenities veto** — needed for the **~90** that leak via Tier-2 and the
  identical-capacity cases `max_guests` can't separate: Jaccard of the normalized
  available-amenity title sets, conservative threshold (start **0.6**, validate).
  Wildcard when either side lacks amenities.

Keyed on `platform` and run before the tier logic, the veto leaves **cross-platform
twin detection untouched** (Booking has neither signal), and the `name ≥ 70` /
`name ≥ 80` duplicate paths still merge genuine same-platform dupes (same flat →
same capacity + ~same amenities).

### Amenities capture (the scrape)
- **Parser** (`src/scrapers/airbnb/parser.py`): `extract_amenities(html)` → sorted
  list of normalized titles (lowercase, stripped) for `AmenityItem`s with
  `available:true`.
- **Scraper** (`src/scrapers/airbnb/scraper.py`): generalize the radius pass into
  one PDP fetch returning **both** `mapMarkerRadiusInMeters` and the amenity titles
  (rename `capture_location_radius` → `capture_pdp_details`); write via dedicated DB
  writers — **no `Listing` dataclass churn** (same approach as radius).
- **Storage** (`src/storage/database.py`): new `amenities` column (JSON list of
  titles; count = length); `set_airbnb_amenities({id: json})`; generalize
  `get_airbnb_listings_missing_radius` → `…_missing_pdp_details` (radius IS NULL OR
  amenities IS NULL); add `amenities` to `_CURATION_COLS`.

- **Curation** (`src/geo/curate.py`): pass `amenities` through to the property-group
  matcher (it's in `_CURATION_COLS`). **Exporter**: add `amenities` to the export.

## "Don't break the deduper" safeguards
- Both new signals are **wildcards for cross-platform pairs** → the 1,495
  cross-platform groups and `recall_proxy = 1.0` are preserved by construction.
- The veto runs **before** the tier logic and is keyed on `platform`, so it gates
  Tier-1 *and* Tier-2 for same-platform pairs while leaving genuine same-platform
  duplicates (same capacity + ~same amenities) and cross-platform twins merging.
- Conservative Jaccard threshold (0.6) + normalized titles to tolerate PDP/caption
  variance across captures.

## Rollout
1. **Part A** (immediate, no scrape): add `max_guests`, re-curate, validate.
2. **Part B**: build capture + discriminator; run the PDP pass over ~6,185 Airbnb
   (multi-hour, monitored); re-curate; validate.

## Verification
- Unit: the same-platform veto in `_compatible` returns False for differing
  `max_guests` and for low-Jaccard amenities, is a no-op for cross-platform pairs
  and for missing signals, and fires in **both** a Tier-1 (relaxed) and a Tier-2
  (strict, name ≥ 80) scenario; `extract_amenities` parses the `AmenityItem`
  fixture; the Jaccard helper is correct. Full suite green.
- E2E after re-curate: the reported pair has **different** `property_group_id`;
  cross-platform group count ≈ 1,495 (unchanged) and `recall_proxy = 1.0`;
  within-Airbnb over-merge groups drop; spot-check a sample of newly-split groups
  are genuinely distinct units.

## Non-goals
- No change to cross-platform (Tier-0/Booking↔Airbnb) logic; no Booking amenities;
  no `Listing`-dataclass change; the Jaccard threshold is tunable, not sacred.
