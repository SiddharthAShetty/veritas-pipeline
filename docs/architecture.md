# Solution Architecture — Veritas Medical Data Standardisation Pipeline

See `architecture_diagram.svg` in this folder for the full diagram. This document is the accompanying narrative.

## 1. Shape of the problem, and the pattern it implies

This is **per-file batch processing disguised as a streaming problem**. Each JSON file is a
self-contained unit of work with no ordering dependency on any other file — clinic A's file
doesn't need to wait for clinic B's, and even within one clinic, one visit's report doesn't
depend on another. That makes this a good fit for **event-driven, per-object processing**
(GCS finalize event → one worker invocation per file) rather than a scheduled batch job that
re-scans the whole bucket. Per-file processing also directly buys us NFR-3.1 (fault isolation)
for free — one malformed file is one failed invocation, not one failed batch of 10,000.

It's also **schema-on-read at the ingestion boundary, schema-on-write at the storage boundary**.
We deliberately do *not* try to define one rigid input schema for 500+ clinics — that's what
NFR-2.1 (zero-code onboarding) rules out. Instead, each clinic's file is read according to its
own config-declared shape, and only the *output* of standardisation is schema-on-write (a fixed
canonical table). This is the core architectural decision the whole design hangs off.

## 2. Ingestion layer (FR-1)

Clinics write JSON directly to a GCS bucket organised as `raw-reports/{clinic_id}/{date}/{file}.json`
(stated assumption). A GCS object-finalize event, via Eventarc, publishes to Pub/Sub, which
triggers a Cloud Run worker per file. Cloud Run over Cloud Functions here mainly for local
parity — the same container that runs in production is the one you can `docker run` locally,
which matters for debugging a 500-clinic long tail of edge cases.

**Clinic identity comes from the folder path, not from sniffing the JSON.** This mirrors the
stated assumption ("organised by clinic ID and date") and is more robust than shape-detection:
two clinics can legitimately share the same upstream OCR/extraction vendor and therefore emit
byte-identical envelope shapes (this repo's `apollo_diagnostics` and `wellness_diagnostics`
configs are exactly this case) — shape alone can't disambiguate them. Shape-matching is kept
as a secondary sanity check (logs a warning, doesn't block) to catch a misrouted file.

**Deduplication (FR-1.2)** checks two independent signals, either of which flags a
duplicate: a clinic-configured identity key (default: `document_id` + `correlation_id`,
joined and hashed — every declared field must match, not just one) and a hash of the
clinical *payload* alone (excluding envelope ids), which catches the case actually observed
in the real sample set — identical clinical content resubmitted under a brand-new
document/correlation/claim id. A known gap: a file sharing only *one* of the declared id
fields with a prior submission, with different content, isn't caught by either signal —
that pattern is genuinely ambiguous (duplicate vs. legitimate correction) and deliberately
isn't auto-suppressed; see Assumptions. The dedup key list and the on/off switch are both
config, not code (`dedup_key_fields` per clinic, `--dedup-enabled` flag).

## 3. Processing layer (FR-2, FR-3)

Three composable stages, each a pure function of (raw record, config) → (canonical rows):
**Standardisation** (test-name fuzzy match, numeric parsing, unit conversion, demographic
normalisation, medicine mapping) → **Validation** (range check, outlier check, analytics
classification). None of these stages hold clinic-specific branches in code — every field
lookup goes through `pathutil.get_path()` against a config-declared dotted path. This is what
makes "add a clinic" or "add a test" a config-only change (see `config/clinics/*.json`,
`config/test_dictionary.json`).

Test-name matching is exact-alias-first, fuzzy-fallback (`difflib`, configurable threshold),
and — critically — **never silently drops an unresolved name**. It's logged to a per-clinic
unresolved-name counter, which is what NFR-4.1's "98% resolved within 30 days" is measured
against in production (a curation queue, not a black hole).

## 4. Storage layer (FR-4)

**BigQuery** in production (SQLite in this prototype — same schema, see Assumptions).
One wide, denormalised `clinical_records` table (a pragmatic subset of the ideal 78-column
schema — see Assumptions/Data), partitioned by `processed_at` date and clustered by `clinic_id`,
since almost every real query ("show me clinic X's last week") filters on both. Loading is a
`MERGE`/upsert keyed on a **deterministic id** derived from
`(document_id, record_type, test_name_original, page_number)` rather than a random UUID —
re-running the pipeline on the same file overwrites the same rows instead of duplicating them
(NFR-3.2). Raw JSON is retained in a parallel `audit-trail/` GCS prefix keyed by `document_id`
(FR-4.3), so any standardisation decision is traceable back to source.

## 5. Configuration layer (NFR-2.1)

A versioned GCS `config/` prefix (mirrored locally in `config/`), loaded fresh by each worker
invocation (Cloud Run cold-starts are frequent enough that this is effectively "hot reload"
without extra machinery). Four config families: **clinic mappings** (field paths, date
formats, envelope shape), **test-name dictionary** (canonical name + aliases + unit +
value-type), **reference ranges** (per-test low/high + outlier bounds), **medicine mapping**
(brand → generic). Schema versioning (NFR-2.3) is handled by filename suffix
(`clinic_id_v2.json`) with the ingestion path falling back to `v1` if the fielded version
isn't declared — avoids a migration framework for what's fundamentally a config diff.

## 6. Error handling (FR-4.2, NFR-3.1)

Two tiers of failure, both non-blocking: **file-level** (malformed JSON, unknown clinic
folder) never reaches the standardisation stage and is dead-lettered immediately; **row-level**
(unresolved test name, non-numeric-where-numeric-expected) doesn't fail the *file* — the rest
of that file's rows still load, and the specific row is flagged (`test_analytics = Invalid` /
`Unclassified`) rather than dropped. A dead-letter Pub/Sub topic fans out to a GCS
`dead-letter/` prefix (for reprocessing after a config fix) and a BigQuery table (for the
dashboard's error-rate metric). Cloud Monitoring alerts when error rate > 1% or p95 lag
exceeds 15 minutes, per NFR-1.2/NFR-5.1.

## 7. UI layer (FR-5)

A Cloud Run-hosted Flask app reading BigQuery views (this prototype: reading SQLite directly —
same route/template code, only the data-access layer changes). Four screens map 1:1 to FR-5.1–5.4:
run dashboard, clinic quality summary, flagged-records queue (filterable by flag type and
clinic), and a record inspector showing raw JSON beside standardised output for a given
document. IAP-gated in production since this surfaces PII.

## Key trade-offs

- **Per-file events over micro-batching**: simpler failure isolation, at the cost of more
  invocations at 200k/day (~2.3/sec average) — well within Cloud Run's autoscale envelope, so
  the complexity of batching wasn't worth it at this volume.
- **One flat table over a normalised star schema**: faster to query for the claims-analyst
  use case ("show me all Hemoglobin results above range this week") without joins; costs
  storage (denormalised demographics repeated per test row) and update complexity if a
  patient's demographics change after their tests are loaded. Production would likely split
  into `reports` / `test_results` / `discharge_details` / `medications` once query patterns
  stabilise.
- **Config-file-per-clinic over a generic rules engine**: a JSONPath-style engine would be
  more "elegant" but harder for a non-engineer (or an LLM-assisted onboarding flow) to author
  and review; flat dotted-path configs are diffable and low-ceremony for the common case.
