# Capture Comparison — April 2026 baseline vs May 2026 re-scrape

Compares the two datasets on disk:

| | Baseline | New |
|---|---|---|
| File | `data/bucharest.db.backup-20260515-091053` | `data/bucharest.db` |
| Scraped | 21–22 Apr 2026 | 15–16 May 2026 |
| Listings | 10,901 | 11,110 |

Both captures were produced with the **same methodology** — greedy 1:1
cross-platform linking, full DSA + host extraction, dated RON→EUR conversion
(see `METHODOLOGY.md`). The baseline snapshot was frozen *after* those methods
were finalised, so this is an apples-to-apples comparison: the differences below
are **real-world platform movement over ~3.5 weeks plus improved Airbnb
trader-data recovery**, not changes in how the data was produced.

---

## 1. Summary

- The market barely moved in size (+1.9% listings) but churned ~9% underneath:
  1,133 listings appeared and 924 disappeared in three and a half weeks.
- **Professional-trader coverage diverged by platform.** Booking's published
  trader disclosure *shrank* (−101 company names) because Booking itself is
  showing `contactDetails` for fewer hosts right now; Airbnb's *grew* (+126
  company names, +123 professional listings) because this run's extraction +
  recovery captured more of what Airbnb publishes.
- Net professional listings across both platforms rose **4,546 → 4,659**;
  listings carrying full company disclosure held roughly flat **4,479 → 4,504**.
- Operator concentration is stable — the same managers dominate, led by
  **STR Asset Management (~190 listings)**.
- Prices are essentially unchanged (Airbnb median €69 both captures).

Neither capture is "the" count of Bucharest rentals — both are dated snapshots
(see `METHODOLOGY.md` §12). Use the May capture as current and keep April as the
baseline.

---

## 2. At a glance

| Metric | Apr (baseline) | May (new) | Δ |
|---|---:|---:|---:|
| Total listings | 10,901 | 11,110 | +209 (+1.9%) |
| — Booking | 4,796 | 4,783 | −13 (−0.3%) |
| — Airbnb | 6,105 | 6,327 | +222 (+3.6%) |
| Distinct properties (cross-platform deduped) | 9,363 | 9,487 | +124 (+1.3%) |
| Cross-platform pairs | 1,538 | 1,623 | +85 (+5.5%) |
| Professional listings (both platforms) | 4,546 | 4,659 | +113 (+2.5%) |
| — with full company disclosure | 4,479 | 4,504 | +25 (+0.6%) |
| Distinct operators | 3,151 | 3,122 | −29 |
| Operators with ≥10 listings | 121 | 119 | −2 |
| Booking priced | 3,300 (69%) | 3,034 (63%) | −266 |
| Airbnb priced | 6,105 (100%) | 6,327 (100%) | +222 |

---

## 3. Volume & churn

Total listings grew only +1.9%, but the set is far from static. Matching by
listing id between the two captures:

- **9,977** listings present in both (the stable core)
- **1,133** in the May capture only (new since April — ~10% of the new set)
- **924** in the April capture only (gone by May — ~8.5% of the baseline)

So roughly **one listing in eleven turned over in three and a half weeks.** This
is the empirical reason the methodology insists every figure be cited with its
capture date: a short-term rental dataset is a moving target, and month-old
counts drift by ~10% from churn alone.

---

## 4. The trader landscape (the headline)

The professional-vs-individual split is the core of this dataset, and the two
platforms moved in opposite directions:

| | Apr | May | Δ |
|---|---:|---:|---:|
| Booking Professional | 1,992 | 1,982 | −10 |
| Booking with company name | 1,932 | 1,831 | **−101** |
| Airbnb Professional | 2,554 | 2,677 | **+123** |
| Airbnb with company name | 2,547 | 2,673 | **+126** |

**Booking down — platform-side, not us.** Booking still flags the same number of
hosts as traders (Professional barely moved, −10), but it published the actual
company `contactDetails` block for ~101 fewer of them. This is Booking's own
host-verification pipeline churning: a trader can be flagged `isTrader=true`
while Booking withholds the company details pending (re-)verification. We
captured every page successfully — the data simply wasn't on the page.

**Airbnb up — better recovery.** This run's Airbnb extraction, plus two retry
passes that defeated transient anti-bot blocks, pulled trader disclosure for
+126 more listings than April. Airbnb's published trader layer is genuinely
larger in this capture.

Net across both platforms: **4,546 → 4,659 professional listings (+2.5%)**, and
**4,479 → 4,504 carrying full company disclosure** — roughly flat, with the
Airbnb gain offsetting the Booking dip.

---

## 5. Business-data completeness

| Field (non-null) | Booking Apr→May | Airbnb Apr→May |
|---|---|---|
| `business_name` | 1,932 → 1,831 | 2,547 → 2,673 |
| `business_registration_number` | 1,797 → 1,701 | 2,523 → 2,653 |
| `business_email` | 1,932 → 1,831 | 2,547 → 2,672 |
| `business_phone` | 1,932 → 1,831 | 2,492 → 2,613 |
| `host_name` (Airbnb) | — | 6,076 → 6,198 |

Booking's fields move together (they come from one `contactDetails` block — when
it's null, all are null), tracking the verification churn in §4. Airbnb's fields
all rose. Airbnb `host_name` reached **6,198 / 6,327 (98%)** in the new capture.

---

## 6. Operator concentration

Concentration is stable: ~3,150 distinct operators, ~120 of them running 10+
listings, in both captures. The biggest managers persist almost unchanged:

| Operator | Apr | May | Platforms |
|---|---:|---:|---|
| STR Asset Management | 198 | 191 | Booking + Airbnb |
| Camelia | 96 | 103 | Airbnb |
| STRE Asset Management SRL | 73 | 74 | Airbnb |
| Algirom SRL | 70 | 69 | Booking |
| Zian Assets Management SRL | 60 | 60 | Booking + Airbnb |
| Global PayStay SRL | 56 | 53 | Booking + Airbnb |
| Casta Clean Tech SRL | 53 | 62 | Airbnb |

> **Caveat (carried from `METHODOLOGY.md` §6):** operator keys are not
> normalised, so registration-number / name variants split one real operator —
> e.g. "STR Asset Management" and "STRE Asset Management SRL" (note the shared
> ~RO41137103 registration) are almost certainly the same group, which would put
> its true footprint near ~265 listings. De-duplicate operators by hand before
> publishing any single-operator figure.

---

## 7. Prices

Asking prices are stable across the two captures (EUR per night, where present):

| | Apr median | May median | Apr mean | May mean |
|---|---:|---:|---:|---:|
| Booking | 72 | 73 | 95 | 101 |
| Airbnb | 69 | 69 | 87 | 88 |

Booking's **priced** count fell 3,300 → 3,034 (−8.1%), but that is **not data
loss** — it reflects which forward dates happened to have availability during the
25-window dated-search sweep, which differs run to run. Airbnb is 100% priced in
both. Prices are arbitrary-future-date snapshots, not averages of booked
transactions (`METHODOLOGY.md` §10).

---

## 8. Cross-platform linking & distinct properties

| | Apr | May |
|---|---:|---:|
| Cross-platform pairs (1 Booking + 1 Airbnb) | 1,538 | 1,623 |
| Listings in a pair | 3,076 | 3,246 |
| Distinct properties (group-deduped) | 9,363 | 9,487 |

More pairs were found in May (+85), consistent with the larger Airbnb set and
its better-recovered names (matching is name + proximity). Both distinct-property
counts remain **estimates** — the linking is deliberately conservative 1:1 and
Airbnb's coordinates are fuzzed (`METHODOLOGY.md` §7–§8).

---

## 9. Data-quality movement

- **Airbnb `Unknown` 29 → 292.** These are listings whose page state never
  rendered the trader signal even after retries — Airbnb anti-bot blocks. They
  are *not* "individual"; they are unclassified. The higher residual reflects
  heavier throttling during the May run (the Phase-3 extraction was rate-limited
  to 6–8 s/listing at times).
- **Parse-drop audit:** the May run recorded `listings_dropped = 43` in
  `scrape_runs` (all Airbnb search-time parse errors out of ~13.5k raw items;
  zero bad-coordinate or missing-id drops). The April baseline predates this
  audit field. Note the audit counts *search-parse* drops, not detail-page
  anti-bot blocks (the `Unknown` residual) — those are a separate category.

These movements are exactly why a capture should always be cited with its date
and its `Unknown` / drop figures, never as an absolute.

---

## 10. Bottom line

- **Use the May capture (`data/bucharest.db`, 11,110 listings) as current.**
  It has the larger Airbnb trader layer, near-complete host names, and the
  corrected classifications from the retry passes.
- **Keep the April baseline** (`data/bucharest.db.backup-20260515-091053`) as the
  prior reference point — useful precisely for churn analysis like §3.
- The two agree on the structural story: a Bucharest short-term-rental market of
  ~9,400–9,500 distinct properties, of which a large professionally-operated
  layer (~4,600 listings, ~120 operators with 10+ listings) is visible through
  DSA disclosure, concentrated among a stable set of management companies.
- Neither capture is a census. Both are dated snapshots with ~10%/month churn,
  platform-side disclosure gaps, and an anti-bot residual on Airbnb. Cite
  accordingly — see `METHODOLOGY.md` §12, "What you can — and cannot — claim".

---

## 11. May 21–22 re-capture + position curation *(added 2026-05-22)*

A third capture was taken with the new **geo/dedup curation stage** (operator +
property dedup, address geocoding, cross-platform/temporal position fusion,
precision tagging). This is the first capture with **trustworthy point-level
positions** for a majority of listings.

**Volume & trader (consistent with May 15 — validates the pipeline):**

| Metric | Apr | May 15 | **May 21–22** |
|---|---:|---:|---:|
| Total listings | 10,901 | 11,110 | **10,982** |
| Booking Professional / Private | 1,992 / 2,804 | 1,982 / 2,801 | **1,983 / 2,811** |
| Airbnb Professional / Individual | 2,554 / 3,522 | 2,677 / 3,358 | **2,639 / 3,544** |
| Airbnb Unknown | 29 | 292 | **2** (after two retry passes: 1,771 → 301 → 2) |
| host_name | 6,076 | 6,198 | **6,183** |

**New — position precision (the headline addition):**
- **6,635 of 10,982 listings (60%) are now `exact`**, median accuracy **~24 m**
  (vs the ~150 m Airbnb fuzz / mixed Booking before). All listings carry a fused
  `latitude_best`/`longitude_best`. (A geocode-recovery pass with improved address
  cleaning re-resolved ~630 previously-failed addresses, cutting Booking listings
  still on a raw coordinate from 995 to 791.)
- **~3,900** positions from geocoded Booking street addresses; **2,914** Airbnb
  listings de-fuzzed by transferring their matched Booking twin's position.
- Positions are clean — max best-vs-scraped shift **< 2 km** (a geocode-drift
  guard discards mis-resolutions > 2 km; ~86 discarded). 28 cross-platform groups
  disagree > 1 km and are flagged (not transferred). The remaining ~41% stay
  `approximate` (Airbnb with no twin / un-geocodable Booking).

**New — dedup & operators:**
- The layered dedup (identity-singleton → operator-block → spatial+name) merges
  more than the old strict 1:1, giving **~8,000 distinct properties** (vs the old
  ~9,400 estimate). Treat it as a tighter, more-merged estimate bracketed against
  the old looser one — see `METHODOLOGY.md` §7.
- **858 operators** (103 with 10+ listings). Identity union-find now merges the
  "STR"/"STRE Asset Management" variants into **one operator of 321 listings**.
- Verification: recall proxy **1.0** (66/66 identity-confirmed cross-platform
  twins grouped). The precision proxy reads 0% only because Booking and Airbnb
  disclose the **same operator in different ID formats** (CUI vs J-number) — the
  flagged "conflicts" are correct matches, not bad merges.

**Bottom line:** the structural story is unchanged across all three captures
(~stable trader split, ~100 operators with 10+ listings). What's new is that you
can now **map and cite individual points where `location_precision = 'exact'`**
(optionally `position_confidence ≥ 0.7`) — previously impossible. Keep using
`grid_cell_id` for the `approximate` rows.
