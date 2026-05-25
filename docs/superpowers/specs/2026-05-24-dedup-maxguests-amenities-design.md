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

### Part A — `max_guests` in the room-config match (free; fixes the reported case)
- `room_config_matches(a, b)` (`src/dedup/property_groups.py`): add `max_guests`
  to the compared fields. `None` stays a wildcard, so **cross-platform pairs are
  unaffected** (Booking never reports `max_guests`). For same-platform pairs,
  12 ≠ 14 → no room match → name sim (~45%) too low → the two separate.

### Part B — amenities capture + discriminator (the scrape)
- **Capture** (`src/scrapers/airbnb/{parser,scraper}.py`): generalize the existing
  radius pass into one PDP fetch that returns **both** `mapMarkerRadiusInMeters`
  *and* the set of available amenity titles. New `parser.extract_amenities(html)`
  → sorted list of normalized titles (lowercase, stripped) for `AmenityItem`s with
  `available:true`. The scraper method (rename `capture_location_radius` →
  `capture_pdp_details`) writes both via dedicated DB writers — **no `Listing`
  dataclass churn** (same approach as radius).
- **Storage** (`src/storage/database.py`): new `amenities` column (JSON-encoded
  sorted title list; count = length). New `set_airbnb_amenities({id: json})`;
  generalize `get_airbnb_listings_missing_radius` → `…_missing_pdp_details`
  (radius IS NULL OR amenities IS NULL). Add `amenities` to `_CURATION_COLS`.
- **Discriminator** (`src/dedup/property_groups.py`): `amenities_compatible(a, b)`
  — for pairs where **both** have an amenity set, require Jaccard overlap of the
  title sets ≥ a tuned threshold (start **0.6**, validate); if either lacks
  amenities (e.g. any cross-platform pair — Booking has none), it's a **wildcard
  (True)**. Fold it into the Tier-1 **room-only** path only:

  **same-platform Tier-1 rule →** `dist ≤ 250 AND (name ≥ 70 OR (room_match[+max_guests] AND amenities_compatible))`.

  So `name ≥ 70` genuine duplicates still merge; two identical-capacity units whose
  amenity sets diverge no longer do. Cross-platform is unchanged (amenities +
  max_guests both wildcard there).

- **Curation** (`src/geo/curate.py`): pass `amenities` through to the property-group
  matcher (it's in `_CURATION_COLS`). **Exporter**: add `amenities` to the export.

## "Don't break the deduper" safeguards
- Both new signals are **wildcards for cross-platform pairs** → the 1,495
  cross-platform groups and `recall_proxy = 1.0` are preserved by construction.
- The amenity check only *tightens* the room-only merge path; the name-≥70 path
  (genuine duplicates) is untouched.
- Conservative Jaccard threshold (0.6) + normalized titles to tolerate PDP/caption
  variance across captures.

## Rollout
1. **Part A** (immediate, no scrape): add `max_guests`, re-curate, validate.
2. **Part B**: build capture + discriminator; run the PDP pass over ~6,185 Airbnb
   (multi-hour, monitored); re-curate; validate.

## Verification
- Unit: `room_config_matches` rejects differing `max_guests`; `extract_amenities`
  parses the `AmenityItem` fixture; `amenities_compatible` (Jaccard, wildcard when
  absent); same-platform Tier-1 rule blocks differing-amenity units, keeps
  name-≥70 dupes; cross-platform pair unaffected. Full suite green.
- E2E after re-curate: the reported pair has **different** `property_group_id`;
  cross-platform group count ≈ 1,495 (unchanged) and `recall_proxy = 1.0`;
  within-Airbnb over-merge groups drop; spot-check a sample of newly-split groups
  are genuinely distinct units.

## Non-goals
- No change to cross-platform (Tier-0/Booking↔Airbnb) logic; no Booking amenities;
  no `Listing`-dataclass change; the Jaccard threshold is tunable, not sacred.
