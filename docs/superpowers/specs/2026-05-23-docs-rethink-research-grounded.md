# Research-grounded rethink of NEW_VS_OLD + METHODOLOGY — design

*2026-05-23*

## Context

Both `NEW_VS_OLD.md` (untracked report) and `METHODOLOGY.md` (tracked) grew
piecemeal as the geo-precision / identity-dedup / Airbnb-radius work landed. The
user wants a **full rethink + update of both**, grounded in **online research**,
and "going fully in." Research (below) both *validates* our core interpretation
and supplies external grounding. The reframe (approach A, approved): ground the
trader data in DSA/EU/Romania law, and **reframe the Airbnb position story around
Airbnb's documented host privacy setting** (Precise vs Approximate location)
rather than presenting de-fuzzing as our own inference. The capture (21–22 May
2026) sits **three days after EU Reg 2024/1028 became applicable (20 May 2026)** —
a genuine regulatory inflection point worth foregrounding.

No code changes. `METHODOLOGY.md` is committed; `NEW_VS_OLD.md` stays untracked
(per the saved no-report-commits preference).

## Research findings (external grounding) + sources

1. **DSA Art. 30 (traceability of traders).** Marketplaces must collect and
   **display on the listing**, "in a clear, easily accessible and comprehensible
   manner," a trader's name, address, phone, email, ID, and **trade-register
   number**; Art. 30(2) requires only "best efforts" to assess reliability — which
   explains incomplete / `contactDetails:null` rows. Our dataset is precisely this
   Art-30 disclosure, aggregated. Source: https://www.eu-digital-services-act.com/Digital_Services_Act_Article_30.html
2. **Airbnb location model — validates the radius finding.** Airbnb hosts choose
   **Precise location** (an exact **pin**; street address withheld until booking)
   or **Approximate location** (a shaded **circle** the unit sits within). This is
   exactly `mapMarkerRadiusInMeters`: **0 = Precise, ~152/500 = Approximate**.
   "~49% of captured Airbnb listings are radius-0" = ~half of hosts opted into
   Precise location. Source: https://www.airbnb.com/help/article/2141
3. **Inside Airbnb + re-identification.** Fuzz is **0–150 m**; same-building units
   anonymized individually (→ scattered). A Harvard study ("A Host of Troubles")
   found the nearest resident is the true host **94%** of the time — the fuzz is
   weak, so cross-platform de-fuzzing is both feasible and a privacy caveat to
   state. Sources: https://insideairbnb.com/data-assumptions/ ,
   https://techscience.org/a/2018100902/
4. **EU Reg 2024/1028.** Applicable **20 May 2026**; requires platforms to
   collect/display a per-unit **registration number** and share activity data
   monthly with Member States via a single digital entry point. Our capture is a
   snapshot at this moment. Source: https://eur-lex.europa.eu/eli/reg/2024/1028/oj/eng
5. **Romania regime.** Hosts need a Ministry-of-Tourism classification
   certificate; ANAF is scrutinising ~23,000 hosts for undeclared income; fines
   €2k–8k. Contextualises the operator/registration layer. The Booking-CUI vs
   Airbnb-J-number split remains our own empirical observation (not externally
   sourced). Source: https://www.romania-insider.com/apartment-rent-airbnb-romania-registration

## Canonical figures (use these exact numbers in BOTH docs — May 2026 capture)

- Listings **10,982** (Booking 4,797 / Airbnb 6,185).
- `exact` positions **8,576 (78%)**; `approximate` 2,406; median accuracy **~21 m**.
- Airbnb radius captured **6,122/6,185 (99%)**, **63 blocked**; **radius-0 = 2,978 (~49%)**.
- Exact-position sources: Booking geocoded address **~2,837** + Booking precise own coord **~1,711**; Airbnb own coord (radius-0 Precise + group fusion) **~3,000** + Airbnb via Booking twin **~1,028**.
- Operators **859** (103 with ≥10 listings); distinct properties **~8,036**; property groups **2,094** (1,495 cross-platform).
- Trader: Professional **4,622** (Booking 1,983 / Airbnb 2,639); Booking Private 2,811; Airbnb Individual 3,544; **Unknown 2**. `business_name` 4,472; `host_name` 6,183.
- OLD baseline (April, `backup-20260515-091053`): 10,901 listings; 0 exact positions; ~9,363 distinct (strict 1:1); 3,151 crude-key operators; Unknown 29.

## NEW_VS_OLD.md — changes (untracked rewrite)

- **Header/basis:** unchanged spine (OLD April baseline vs NEW current).
- **§1 Headline + §3 positions:** reframe lead — Airbnb obfuscation is a host
  *privacy setting* (Precise vs Approximate); ~half of Bucharest hosts chose
  Precise, so the exact pin was *published, not inferred*. Update to 78% / ~21 m,
  the 4-source breakdown above, and the pin-fix note. Cite Airbnb Help 2141 + the
  150 m / 94% context.
- **New short section "Regulatory moment":** the capture lands 3 days after EU Reg
  2024/1028 took effect + amid Romania's registration crackdown + DSA Art. 30 is
  why the trader data exists at all. Why this snapshot matters now.
- **§4 operators / §5 stable / §6 bottom line:** refresh figures; keep the
  "different metric" caveat for 3,151→859.

## METHODOLOGY.md — changes (tracked)

- **§1:** add the one-line regulatory-moment framing (capture vs Reg 2024/1028).
- **§3 (legal basis):** cite DSA Art. 30 (the exact required fields + display
  duty = our data); add EU Reg 2024/1028 (registration-number regime, effective
  at capture) and the Romania regime. Keep the "unverified" caveat.
- **§7 (dedup):** one-line benchmark — Inside Airbnb links by listing id only;
  we add operator union-find + cross-platform property grouping.
- **§8 (geo):** ground the radius interpretation in Airbnb's Precise/Approximate
  setting (0 = Precise pin = the listed coord is the true point; >0 = Approximate
  circle); cite Inside Airbnb's 150 m + individual-building scatter and the 94%
  re-id study (privacy caveat + why twin de-fuzzing works). Refresh to 8,576/78%/21 m.
- **New "Sources & related work" section** (near the end): the five sources above.
- Sweep for stale figures; reconcile to the canonical set.

## Non-goals

- No full restructure of either doc; no code changes; no re-verification of data
  (figures locked above). No new external claims beyond the five sourced areas.

## Verification

- Every figure traces to the canonical table (or a re-runnable DB query).
- Every external/legal claim carries one of the five cited sources; nothing
  asserted about the law beyond what the source supports.
- Both docs render as GitHub-flavoured Markdown; internal consistency (figures
  agree across sections and between the two docs); METHODOLOGY committed,
  NEW_VS_OLD left untracked.
