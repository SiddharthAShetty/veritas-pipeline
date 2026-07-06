"""
Database loader.

Implements:
  FR-4.1  Structured DB load  -- SQLite for the take-home (see Assumptions
          doc for the BigQuery/Postgres production mapping); schema mirrors
          the canonical column spec (a pragmatic subset of
          Ourput-table-ideal-schema.csv -- see docs/assumptions.md).
  NFR-3.2 Idempotency         -- re-running the pipeline on the same input
          must not create duplicate rows. Enforced two ways: (a) ingestion-
          level dedup (FR-1.2) suppresses duplicate source files before
          they ever reach this module, and (b) this module additionally
          upserts on `id` derived deterministically from
          (document_id, correlation_id, test_name_original, page_number,
          record_type) rather than a random UUID, so a second run of the
          same file produces the same row ids and therefore overwrites,
          not duplicates.
  FR-4.2  Error logging       -- write_errors() appends to a dead-letter
          JSONL file with the failure reason, for manual review /
          reprocessing.
  FR-4.3  Audit trail         -- raw JSON is retained alongside the
          transformed record (see write_audit_trail()).
"""
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

CANONICAL_COLUMNS = [
    # Metadata
    "id", "document_id", "correlation_id", "trace_id",
    "clinic_id", "source_system", "source_system_reported",
    "claim_no", "nt_code", "consumer_client_id",
    "destination_identifier", "file_gcs_path", "record_type",
    "metadetails",

    # Patient Information
    "patient_name", "uhid",
    "gender",
    "age_text", "age",
    "age_years", "age_months", "age_days",
    "basic_info_age",

    # Hospital Information
    "hospital_name", "lab_or_hospital_name",
    "hospital_address", "doctor_name", "ward",

    # Dates
    "bill_date", "basic_info_bill_date",
    "reports_date", "report_date",
    "admission_date", "discharge_date",

    # Test Details
    "test_name_canonical",
    "test_name_original",
    "test_name",
    "report_details_test_name",

    "result_value",
    "result_text",
    "result",
    "result_text_original",
    "report_details_result",

    "unit_canonical",
    "unit_original",
    "unit",
    "report_details_unit",

    "range_low",
    "range_high",
    "range_text",
    "range",
    "range_text_original",
    "report_details_range",
    "range_source",

    # Analytics
    "test_analytics",
    "report_details_test_analytics",
    "flag_reason",
    "normalization_method",
    "normalization_confidence",
    "duplicate_within_report",

    # Page Information
    "page_number",
    "page_no",
    "report_details_page_no",

    # Clinical Notes
    "diagnosis",
    "brief_history",
    "general_examinations",
    "course_during_hospitalisation",
    "course_during_hospitalization",  # American spelling
    "recommendations",
    "post_discharge_advice",

    # Medicines
    "medicine",
    "medication_name",
    "medication_medicine",
    "generic_name",
    "drug_class",
    "medicine_type",

    "dose",
    "medication_dose",
    "discharge_medications_dose",

    "frequency",
    "medication_frequency",
    "discharge_medications_frequency",

    "discharge_medications_medicine",
    "medicine_injections_investigation",
    "other_med_inj_investigations",

    # Timestamps
    "ingested_at",
    "processed_at",
]

_TYPE_OVERRIDES = {
    "result_value": "REAL", "range_low": "REAL", "range_high": "REAL",
    "normalization_confidence": "REAL",
    "age_years": "REAL", "age_months": "REAL", "age_days": "REAL",
}


def _sql_safe(value: Any) -> Any:
    """Some source clinics send list-typed fields (e.g. a list of injection/
    investigation strings). SQLite can't bind Python lists/dicts directly,
    so flatten them to a readable string rather than dropping the data or
    crashing the load. Scalars pass through untouched."""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value) if value else None
    if isinstance(value, dict):
        return json.dumps(value)
    return value


def deterministic_row_id(row: Dict[str, Any]) -> str:
    """Stable id so re-processing the same file overwrites instead of
    duplicating (NFR-3.2). Falls back gracefully when document_id is
    absent (shouldn't happen post-ingestion, but defensive)."""
    key_parts = [
        str(row.get("document_id") or row.get("file_gcs_path") or ""),
        str(row.get("record_type") or ""),
        str(row.get("test_name_canonical") or row.get("test_name_original") or ""),
        str(row.get("result_value") if row.get("result_value") is not None else row.get("result_text") or ""),
        str(row.get("unit_canonical") or row.get("unit_original") or ""),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


class SQLiteLoader:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_schema()

    def _add_missing_columns(self, table: str, expected_columns: Dict[str, str]):
        existing = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        for col, col_type in expected_columns.items():
            if col not in existing:
                self.conn.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {col_type}')

    def _create_schema(self):
        cols_sql = ", ".join(
            f'"{c}" {_TYPE_OVERRIDES.get(c, "TEXT")}' for c in CANONICAL_COLUMNS
        )
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS clinical_records ({cols_sql}, PRIMARY KEY (id))")
        self._add_missing_columns("clinical_records", {
            c: _TYPE_OVERRIDES.get(c, "TEXT") for c in CANONICAL_COLUMNS
        })
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT,
                files_seen INTEGER, files_processed INTEGER, files_failed INTEGER,
                duplicates_suppressed INTEGER, rows_loaded INTEGER, rows_flagged INTEGER
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS clinic_quality_stats (
                clinic_id TEXT PRIMARY KEY, files_seen INTEGER, files_failed INTEGER,
                duplicates_suppressed INTEGER, rows_loaded INTEGER,
                unresolved_test_names INTEGER, invalid_rows INTEGER, outlier_rows INTEGER,
                within_report_duplicate_rows INTEGER
            )
        """)
        self._add_missing_columns("clinic_quality_stats", {
            "files_seen": "INTEGER", "files_failed": "INTEGER",
            "duplicates_suppressed": "INTEGER", "rows_loaded": "INTEGER",
            "unresolved_test_names": "INTEGER", "invalid_rows": "INTEGER",
            "outlier_rows": "INTEGER", "within_report_duplicate_rows": "INTEGER",
        })

        # FR-2.2: "For each defined test, produce exactly 5 columns: Test_Name,
        # Test_Name_Result, Test_Name_Range, Test_Name_Unit, Test_Name_Analytics.
        # If a test is absent from a report, leave the columns blank."
        # One row per test result; these 5 column names are fixed and reused
        # for every test (not a separate 5-column block per test name -- see
        # docs/assumptions.md for why this reading was chosen over the
        # alternative wide-table reading). document_id/patient/clinic columns
        # identify *whose* test and *which* report a row belongs to -- they
        # aren't part of the test's own 5-column tuple, just row identity.
        self.conn.execute("DROP VIEW IF EXISTS vw_test_result")
        self.conn.execute("""
            CREATE VIEW vw_test_result AS
            SELECT
                document_id, clinic_id, patient_name, uhid,
                test_name_canonical AS Test_Name,
                CASE WHEN result_value IS NOT NULL THEN CAST(result_value AS TEXT)
                     ELSE result_text END AS Test_Name_Result,
                CASE WHEN range_low IS NOT NULL AND range_high IS NOT NULL
                     THEN CAST(range_low AS TEXT) || '-' || CAST(range_high AS TEXT)
                     ELSE range_text END AS Test_Name_Range,
                COALESCE(unit_canonical, unit_original) AS Test_Name_Unit,
                test_analytics AS Test_Name_Analytics
            FROM clinical_records
            WHERE record_type = 'lab_test'
        """)
        self.conn.commit()

    def upsert_rows(self, rows: Iterable[Dict[str, Any]]) -> int:
        count = 0
        placeholders = ", ".join(f":{c}" for c in CANONICAL_COLUMNS)
        col_list = ", ".join(f'"{c}"' for c in CANONICAL_COLUMNS)
        sql = f"INSERT OR REPLACE INTO clinical_records ({col_list}) VALUES ({placeholders})"
        seen_ids = set()
        for row in rows:
            record = {c: _sql_safe(row.get(c)) for c in CANONICAL_COLUMNS}
            record["id"] = deterministic_row_id(row)

            if record["id"] in seen_ids:
                continue

            seen_ids.add(record["id"])

            self.conn.execute(sql, record)
            count += 1
        self.conn.commit()
        return count

    def upsert_clinic_stats(self, stats: Dict[str, Dict[str, int]]):
        for clinic_id, s in stats.items():
            self.conn.execute("""
                INSERT INTO clinic_quality_stats
                    (clinic_id, files_seen, files_failed, duplicates_suppressed,
                     rows_loaded, unresolved_test_names, invalid_rows, outlier_rows,
                     within_report_duplicate_rows)
                VALUES (:clinic_id, :files_seen, :files_failed, :duplicates_suppressed,
                        :rows_loaded, :unresolved_test_names, :invalid_rows, :outlier_rows,
                        :within_report_duplicate_rows)
                ON CONFLICT(clinic_id) DO UPDATE SET
                    files_seen=excluded.files_seen, files_failed=excluded.files_failed,
                    duplicates_suppressed=excluded.duplicates_suppressed,
                    rows_loaded=excluded.rows_loaded,
                    unresolved_test_names=excluded.unresolved_test_names,
                    invalid_rows=excluded.invalid_rows, outlier_rows=excluded.outlier_rows,
                    within_report_duplicate_rows=excluded.within_report_duplicate_rows
            """, {"clinic_id": clinic_id, **s})
        self.conn.commit()

    def record_run(self, run_stats: Dict[str, Any]):
        self.conn.execute("""
            INSERT OR REPLACE INTO pipeline_runs
                (run_id, started_at, finished_at, files_seen, files_processed, files_failed,
                 duplicates_suppressed, rows_loaded, rows_flagged)
            VALUES (:run_id, :started_at, :finished_at, :files_seen, :files_processed,
                    :files_failed, :duplicates_suppressed, :rows_loaded, :rows_flagged)
        """, run_stats)
        self.conn.commit()

    def close(self):
        self.conn.close()


def write_errors(error_dicts: List[Dict[str, Any]], path: str):
    """FR-4.2: dead-letter store, one JSON object per line, append mode
    so multiple runs accumulate a reviewable history."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for e in error_dicts:
            f.write(json.dumps(e) + "\n")


def write_audit_trail(document_id: str, file_path: str, raw_json: Dict[str, Any],
                       transformed_rows: List[Dict[str, Any]], audit_dir: str):
    """FR-4.3: retain raw JSON alongside the transformed output so any
    standardisation decision can be traced back to source."""
    Path(audit_dir).mkdir(parents=True, exist_ok=True)
    safe_id = (document_id or Path(file_path).stem).replace("/", "_")
    with open(Path(audit_dir) / f"{safe_id}.audit.json", "w", encoding="utf-8") as f:
        json.dump({"source_file": file_path, "raw_json": raw_json,
                    "transformed_rows": transformed_rows}, f, indent=2, default=str)
