"""
Operational UI (FR-5). A deliberately lightweight Flask app -- server-rendered
HTML, no build step, no JS framework -- because the assignment scope calls
for "functional, not production-grade" (FR-5 evaluation criteria). Reads
directly from the SQLite DB the pipeline writes to.

Run:
    python ui/app.py
Then open http://localhost:5000
"""
import json
import sqlite3
from pathlib import Path

from flask import Flask, render_template, request, abort

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "output" / "veritas.db"
ERROR_LOG = BASE_DIR / "output" / "errors" / "dead_letter.jsonl"
AUDIT_DIR = BASE_DIR / "output" / "audit"

app = Flask(__name__)


@app.template_filter("pill_class")
def pill_class(analytics_value):
    mapping = {
        "Outlier": "pill-outlier", "Invalid": "pill-invalid",
        "Above Range": "pill-above", "Below Range": "pill-below",
        "Within Range": "pill-within", "Unclassified": "pill-unclassified",
        "Not Applicable": "pill-na",
    }
    return mapping.get(analytics_value, "pill-unclassified")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_exists():
    return DB_PATH.exists()


# --------------------------------------------------------------------
# FR-5.1 Pipeline dashboard
# --------------------------------------------------------------------
@app.route("/")
def dashboard():
    if not db_exists():
        return render_template("no_data.html")
    conn = get_conn()
    last_run = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1").fetchone()
    total_runs = conn.execute("SELECT COUNT(*) c FROM pipeline_runs").fetchone()["c"]
    total_records = conn.execute("SELECT COUNT(*) c FROM clinical_records").fetchone()["c"]
    flagged_breakdown = conn.execute("""
        SELECT test_analytics, COUNT(*) c FROM clinical_records
        WHERE test_analytics IS NOT NULL GROUP BY test_analytics ORDER BY c DESC
    """).fetchall()
    record_type_breakdown = conn.execute("""
        SELECT record_type, COUNT(*) c FROM clinical_records GROUP BY record_type ORDER BY c DESC
    """).fetchall()
    conn.close()

    error_count = 0
    recent_errors = []
    if ERROR_LOG.exists():
        with open(ERROR_LOG, "r") as f:
            lines = f.readlines()
        error_count = len(lines)
        recent_errors = [json.loads(l) for l in lines[-10:]][::-1]

    return render_template(
        "dashboard.html", active="dashboard", last_run=last_run, total_runs=total_runs,
        total_records=total_records, flagged_breakdown=flagged_breakdown,
        record_type_breakdown=record_type_breakdown,
        error_count=error_count, recent_errors=recent_errors)


# --------------------------------------------------------------------
# FR-5.4 Clinic-level summary
# --------------------------------------------------------------------
@app.route("/clinics")
def clinics():
    if not db_exists():
        return render_template("no_data.html")
    conn = get_conn()
    rows = conn.execute("SELECT * FROM clinic_quality_stats ORDER BY clinic_id").fetchall()
    conn.close()
    return render_template("clinics.html", active="clinics", rows=rows)


# --------------------------------------------------------------------
# FR-5.3 Flagged records queue
# --------------------------------------------------------------------
@app.route("/flagged")
def flagged():
    if not db_exists():
        return render_template("no_data.html")
    flag_filter = request.args.get("flag", "")
    clinic_filter = request.args.get("clinic", "")
    conn = get_conn()
    query = """
        SELECT id, document_id, clinic_id, patient_name, test_name_canonical, test_name_original,
               result_text, result_value, unit_canonical, range_low, range_high,
               test_analytics, flag_reason, normalization_method, processed_at,
               duplicate_within_report
        FROM clinical_records
        WHERE (test_analytics IN ('Outlier','Invalid','Above Range','Below Range','Unclassified')
               OR duplicate_within_report = 1)
    """
    params = []
    if flag_filter == "WithinReportDuplicate":
        query += " AND duplicate_within_report = 1"
    elif flag_filter:
        query += " AND test_analytics = ?"
        params.append(flag_filter)
    if clinic_filter:
        query += " AND clinic_id = ?"
        params.append(clinic_filter)
    query += " ORDER BY CASE test_analytics WHEN 'Outlier' THEN 0 WHEN 'Invalid' THEN 1 ELSE 2 END LIMIT 300"
    rows = conn.execute(query, params).fetchall()
    clinics_list = [r["clinic_id"] for r in conn.execute(
        "SELECT DISTINCT clinic_id FROM clinical_records ORDER BY clinic_id")]
    conn.close()
    return render_template("flagged.html", active="flagged", rows=rows, clinics_list=clinics_list,
                            flag_filter=flag_filter, clinic_filter=clinic_filter)


# --------------------------------------------------------------------
# FR-5.2 Record inspector -- raw JSON alongside standardised output
# --------------------------------------------------------------------
@app.route("/records")
def records_search():
    q = request.args.get("q", "").strip()
    conn = get_conn() if db_exists() else None
    results = []
    if conn and q:
        results = conn.execute("""
            SELECT DISTINCT document_id, clinic_id, patient_name, uhid, hospital_name
            FROM clinical_records
            WHERE document_id LIKE ? OR uhid LIKE ? OR patient_name LIKE ?
            LIMIT 50
        """, (f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
    if conn:
        conn.close()
    return render_template("records_search.html", active="records", results=results, q=q)


@app.route("/records/<document_id>")
def record_detail(document_id):
    if not db_exists():
        return render_template("no_data.html")
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM clinical_records WHERE document_id = ? ORDER BY record_type, page_number",
        (document_id,)).fetchall()
    conn.close()
    if not rows:
        abort(404)

    audit_file = AUDIT_DIR / f"{document_id}.audit.json"
    raw_json_pretty = None
    if audit_file.exists():
        with open(audit_file) as f:
            audit = json.load(f)
        raw_json_pretty = json.dumps(audit.get("raw_json"), indent=2)

    return render_template("record_detail.html", active="records", document_id=document_id,
                            rows=rows, raw_json_pretty=raw_json_pretty)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
