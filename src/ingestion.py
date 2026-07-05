"""
Ingestion module.

Implements:
  FR-1.1 Multi-source ingestion  -- walks a folder tree simulating a GCS
         bucket organised as <clinic_id>/<date>/<file>.json (per the
         assumption stated in the assignment). In production this becomes
         a GCS event trigger (per-file) or a scheduled batch listing --
         see docs/architecture.md.
  FR-1.2 Duplicate detection     -- configurable dedup key per clinic
         (defaults to document_id + correlation_id; falls back to a
         content hash if those are absent). Duplicates are logged, not
         silently dropped, so ops can audit what got suppressed.
  FR-1.3 Schema flexibility      -- clinic identity comes from the folder
         path (matches the GCS assumption), not from sniffing JSON shape.
         The clinic's config, not the code, tells the parser where fields
         live. Unknown clinic folders or content that doesn't match the
         clinic's declared envelope shape are flagged, not crashed on.
"""
import json
import hashlib
import logging
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pathutil import get_path, has_required_keys

logger = logging.getLogger("veritas.ingestion")


@dataclass
class RawRecord:
    clinic_id: str
    file_path: str
    raw_json: Dict[str, Any]
    clinic_config: Dict[str, Any]
    ingested_at: str
    trace_id: Optional[str] = None
    document_id: Optional[str] = None
    correlation_id: Optional[str] = None
    dedup_key: Optional[str] = None
    content_hash: Optional[str] = None
    meta_details: Dict[str, Any] = field(default_factory=dict)


def _parse_meta_details(raw_list) -> Dict[str, Any]:
    """metaDetails arrives as [{"key": ..., "value": ...}, ...] -- flatten to a
    dict. Carries claim_no (the FK back to the insurance claim -- important
    lineage for a claims company, NFR-4.2) and source_system among others."""
    out = {}
    if isinstance(raw_list, list):
        for item in raw_list:
            if isinstance(item, dict) and "key" in item:
                out[item["key"]] = item.get("value")
    return out


@dataclass
class IngestionError:
    file_path: str
    clinic_id: Optional[str]
    reason: str
    stage: str = "ingestion"
    detail: Optional[str] = None
    occurred_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ClinicConfigRegistry:
    """Loads config/clinics/*.json once and serves them by clinic_id.
    Adding a clinic = dropping a new file here. No code change (NFR-2.1)."""

    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        self._configs: Dict[str, Dict[str, Any]] = {}
        self._load_all()

    def _load_all(self):
        for f in sorted(self.config_dir.glob("*.json")):
            with open(f, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            clinic_id = cfg.get("clinic_id") or f.stem
            self._configs[clinic_id] = cfg
        logger.info("Loaded %d clinic configs: %s", len(self._configs), list(self._configs))

    def get(self, clinic_id: str) -> Optional[Dict[str, Any]]:
        return self._configs.get(clinic_id)

    def known_clinics(self) -> List[str]:
        return list(self._configs.keys())


class Ingestion:
    def __init__(self, sample_data_dir: str, clinic_registry: ClinicConfigRegistry,
                 dedup_enabled: bool = True):
        self.root = Path(sample_data_dir)
        self.registry = clinic_registry
        self.dedup_enabled = dedup_enabled  # FR-1.2: "must be configurable"
        self._seen_dedup_keys = set()
        self._seen_content_hashes = set()
        self.errors: List[IngestionError] = []
        self.duplicates_suppressed: List[Dict[str, Any]] = []

    # ---- discovery -------------------------------------------------
    def discover_files(self):
        """Yields (clinic_id, date_str, Path) for every *.json under root,
        following the <clinic_id>/<date>/<file>.json convention."""
        if not self.root.exists():
            logger.warning("Sample data root %s does not exist", self.root)
            return
        for path in sorted(self.root.rglob("*.json")):
            rel = path.relative_to(self.root)
            parts = rel.parts
            if len(parts) >= 3:
                clinic_id, date_str = parts[0], parts[1]
            elif len(parts) == 2:
                clinic_id, date_str = parts[0], "unknown-date"
            else:
                clinic_id, date_str = "unknown-clinic", "unknown-date"
            yield clinic_id, date_str, path

    def _remove_page_no(self, obj):
        if isinstance(obj, dict):
            return {
                k: self._remove_page_no(v)
                for k, v in obj.items()
                if k != "page_no"
            }
        elif isinstance(obj, list):
            return [self._remove_page_no(item) for item in obj]
        return obj    

    # ---- per-file processing ---------------------------------------
    def _content_hash(self, content: Dict[str, Any], cfg: Dict[str, Any]) -> str:
        """Hashes the clinical *payload* (the responseDetails/blocks, or the
        whole body for flat-schema clinics), deliberately excluding envelope
        identifiers (trace_id/document_id/correlation_id/claim_no). Those
        change on every resubmission by design -- hashing them in would
        defeat the point of a content-based duplicate check. This is what
        catches the real case in the sample set: identical clinical content
        resubmitted under a brand-new document_id/correlation_id/claim_no."""
        env = cfg.get("envelope", {})
        blocks_path = env.get("blocks_path")
        payload = get_path(content, blocks_path) if blocks_path else content
        if payload is None:
            payload = content
            
        payload = self._remove_page_no(copy.deepcopy(payload))
       
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _dedup_key_for(self, content: Dict[str, Any], cfg: Dict[str, Any], file_path: Path) -> str:
        key_fields = cfg.get("dedup_key_fields") or []
        env = cfg.get("envelope", {})
        values = []
        for f in key_fields:
            if f == "document_id":
                values.append(str(get_path(content, env.get("document_id_path"), "")))
            elif f == "correlation_id":
                values.append(str(get_path(content, env.get("correlation_id_path"), "")))
            else:
                values.append(str(get_path(content, f, "")))
        joined = "|".join(v for v in values if v)
        if joined:
            return hashlib.sha256(joined.encode("utf-8")).hexdigest()
        # Fallback: hash of raw content -- catches byte-identical resubmits
        # even when a clinic's declared key fields are blank/missing.
        return self._content_hash(content, cfg)

    def process_file(self, clinic_id: str, date_str: str, path: Path) -> Optional[RawRecord]:
        cfg = self.registry.get(clinic_id)
        if cfg is None:
            self.errors.append(IngestionError(
                file_path=str(path), clinic_id=clinic_id,
                reason="unknown_clinic",
                detail=f"No clinic config found for '{clinic_id}'. Onboard via config/clinics/{clinic_id}.json"))
            logger.error("Unknown clinic '%s' for file %s", clinic_id, path)
            return None

        try:
            with open(path, "r", encoding="utf-8") as fh:
                content = json.load(fh)
        except json.JSONDecodeError as e:
            self.errors.append(IngestionError(
                file_path=str(path), clinic_id=clinic_id,
                reason="malformed_json", detail=str(e)))
            logger.error("Malformed JSON in %s: %s", path, e)
            return None
        except Exception as e:  # noqa: BLE001 - deliberately broad at the ingestion boundary
            self.errors.append(IngestionError(
                file_path=str(path), clinic_id=clinic_id,
                reason="unreadable_file", detail=str(e)))
            return None

        # Sanity check: does this file's shape match what the clinic config
        # expects? Doesn't block processing (a clinic may legitimately mix
        # shapes during a schema migration -- see NFR-2.3) but is logged so
        # ops can catch a misfiled / mis-routed submission.
        match_cfg = cfg.get("match", {})
        if match_cfg.get("strategy") == "envelope_shape":
            required = match_cfg.get("required_keys", [])
            if not has_required_keys(content, required):
                logger.warning(
                    "File %s under clinic '%s' does not match declared envelope shape "
                    "(missing one of %s). Proceeding, but flag for review.",
                    path, clinic_id, required)

        env = cfg.get("envelope", {})
        record = RawRecord(
            clinic_id=clinic_id,
            file_path=str(path),
            raw_json=content,
            clinic_config=cfg,
            ingested_at=datetime.now(timezone.utc).isoformat(),
            trace_id=get_path(content, env.get("trace_id_path")),
            document_id=get_path(content, env.get("document_id_path")),
            correlation_id=get_path(content, env.get("correlation_id_path")),
            meta_details=_parse_meta_details(get_path(content, env.get("meta_details_path"))),
        )
        record.dedup_key = self._dedup_key_for(content, cfg, path)
        record.content_hash = self._content_hash(content, cfg)

        if self.dedup_enabled:
            # Two independent dedup signals: the clinic's declared identity
            # key (document_id/correlation_id), and a raw-content hash. The
            # latter catches a case actually observed in the real sample
            # set -- the *same* clinical payload resubmitted wrapped in a
            # new document_id/correlation_id/claim_no. Either match counts
            # as a duplicate.
            if record.dedup_key in self._seen_dedup_keys or record.content_hash in self._seen_content_hashes:
                self.duplicates_suppressed.append({
                    "file_path": str(path), "clinic_id": clinic_id,
                    "dedup_key": record.dedup_key,
                    "reason": "duplicate_document_id_correlation_id_or_content"})
                logger.info("Suppressed duplicate: %s (clinic=%s)", path, clinic_id)
                return None
            self._seen_dedup_keys.add(record.dedup_key)
            self._seen_content_hashes.add(record.content_hash)

        return record

    def run(self) -> List[RawRecord]:
        records = []
        for clinic_id, date_str, path in self.discover_files():
            rec = self.process_file(clinic_id, date_str, path)
            if rec is not None:
                records.append(rec)
        logger.info("Ingestion complete: %d records, %d errors, %d duplicates suppressed",
                    len(records), len(self.errors), len(self.duplicates_suppressed))
        return records
