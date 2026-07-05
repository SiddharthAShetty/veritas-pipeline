"""
Entry point: run the full ingestion -> standardisation -> validation -> load
pipeline over sample-data/, using config/ for every clinic-specific rule.

Usage:
    python src/run_pipeline.py
    python src/run_pipeline.py --sample-data-dir sample-data --db-path output/veritas.db
"""
import argparse
import logging
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ingestion import ClinicConfigRegistry, Ingestion
from standardisation import TestNameMatcher, UnitConverter, MedicineMatcher
from validation import ValidationEngine
from pipeline import StandardisationPipeline
from db_loader import SQLiteLoader, write_errors, write_audit_trail

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("veritas.run_pipeline")

BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-data-dir", default=str(BASE_DIR / "sample-data"))
    parser.add_argument("--config-dir", default=str(BASE_DIR / "config"))
    parser.add_argument("--db-path", default=str(BASE_DIR / "output" / "veritas.db"))
    parser.add_argument("--error-log", default=str(BASE_DIR / "output" / "errors" / "dead_letter.jsonl"))
    parser.add_argument("--audit-dir", default=str(BASE_DIR / "output" / "audit"))
    parser.add_argument("--dedup-enabled", action="store_true", default=True)
    parser.add_argument("--reset", action="store_true",
                         help="Delete the existing DB/audit/error output before running, so "
                              "the result reflects exactly what's in sample-data/ right now "
                              "(no stale rows from clinics/files removed since the last run).")
    args = parser.parse_args()

    if args.reset:
        for p in [args.db_path, args.error_log]:
            Path(p).unlink(missing_ok=True)
        audit_dir = Path(args.audit_dir)
        if audit_dir.exists():
            for f in audit_dir.glob("*"):
                f.unlink()
        logger.info("--reset: cleared previous DB/audit/error output before this run")

    Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("=== Veritas pipeline run %s starting ===", run_id)

    # ---- wire up config-driven components -------------------------
    clinic_registry = ClinicConfigRegistry(f"{args.config_dir}/clinics")
    ingestion = Ingestion(args.sample_data_dir, clinic_registry, dedup_enabled=args.dedup_enabled)

    test_matcher = TestNameMatcher(f"{args.config_dir}/test_dictionary.json")
    unit_converter = UnitConverter(f"{args.config_dir}/unit_conversions.json")
    medicine_matcher = MedicineMatcher(f"{args.config_dir}/medicine_mapping.json")
    validator = ValidationEngine(f"{args.config_dir}/reference_ranges.json", f"{args.config_dir}/test_dictionary.json")
    std_pipeline = StandardisationPipeline(test_matcher, unit_converter, medicine_matcher, validator)

    loader = SQLiteLoader(args.db_path)

    # ---- run ---------------------------------------------------------
    files_seen = sum(1 for _ in ingestion.discover_files())
    records = ingestion.run()

    clinic_stats = defaultdict(lambda: {
        "files_seen": 0, "files_failed": 0, "duplicates_suppressed": 0,
        "rows_loaded": 0, "unresolved_test_names": 0, "invalid_rows": 0, "outlier_rows": 0,
        "within_report_duplicate_rows": 0,
    })
    for clinic_id, _, _ in ingestion.discover_files():
        clinic_stats[clinic_id]["files_seen"] += 1
    for err in ingestion.errors:
        if err.clinic_id:
            clinic_stats[err.clinic_id]["files_failed"] += 1
    for dup in ingestion.duplicates_suppressed:
        clinic_stats[dup["clinic_id"]]["duplicates_suppressed"] += 1

    all_rows = []
    for record in records:
        rows = std_pipeline.process_record(record)
        all_rows.extend(rows)
        write_audit_trail(record.document_id, record.file_path, record.raw_json, rows, args.audit_dir)

        stats = clinic_stats[record.clinic_id]
        stats["rows_loaded"] += len(rows)
        for r in rows:
            if r.get("normalization_method") == "unresolved":
                stats["unresolved_test_names"] += 1
            if r.get("test_analytics") == "Invalid":
                stats["invalid_rows"] += 1
            if r.get("test_analytics") == "Outlier":
                stats["outlier_rows"] += 1
            if r.get("duplicate_within_report"):
                stats["within_report_duplicate_rows"] += 1

    rows_loaded = loader.upsert_rows(all_rows)
    loader.upsert_clinic_stats(dict(clinic_stats))

    # ---- error / dead-letter log --------------------------------------
    error_dicts = [e.__dict__ for e in ingestion.errors] + [e.to_dict() for e in std_pipeline.row_errors]
    if error_dicts:
        write_errors(error_dicts, args.error_log)

    flagged_count = sum(1 for r in all_rows if r.get("test_analytics") in
                         {"Outlier", "Invalid", "Above Range", "Below Range"})

    finished_at = datetime.now(timezone.utc).isoformat()
    loader.record_run({
        "run_id": run_id, "started_at": started_at, "finished_at": finished_at,
        "files_seen": files_seen, "files_processed": len(records),
        "files_failed": len(ingestion.errors),
        "duplicates_suppressed": len(ingestion.duplicates_suppressed),
        "rows_loaded": rows_loaded, "rows_flagged": flagged_count,
    })
    loader.close()

    logger.info("=== Run complete ===")
    logger.info("Files seen: %d | processed: %d | failed: %d | duplicates suppressed: %d",
                files_seen, len(records), len(ingestion.errors), len(ingestion.duplicates_suppressed))
    logger.info("Rows loaded: %d | flagged (outlier/invalid/out-of-range): %d", rows_loaded, flagged_count)
    logger.info("Unresolved test names this run: %d", len(set(test_matcher.unresolved_log)))
    if test_matcher.unresolved_log:
        logger.info("  -> %s", sorted(set(test_matcher.unresolved_log)))
    logger.info("DB: %s | Errors: %s | Audit trail: %s", args.db_path, args.error_log, args.audit_dir)
    logger.info("vw_test_results view available at: SELECT * FROM vw_test_result")


if __name__ == "__main__":
    main()
