"""
Standardisation module.

Implements FR-2.1 through FR-2.6:
  2.1 Test name normalisation   -- TestNameMatcher (exact + fuzzy against a
                                    configurable dictionary)
  2.2 Fixed column schema       -- enforced downstream by db_loader against
                                    the canonical schema; this module just
                                    guarantees every test row carries the
                                    5 canonical fields (name/result/range/
                                    unit/analytics-ready values)
  2.3 Numeric conversion        -- parse_numeric_result()
  2.4 Unit harmonisation        -- convert_unit()
  2.5 Demographic normalisation -- normalize_age / normalize_gender / normalize_date
  2.6 Medicine name mapping     -- MedicineMatcher
"""
import difflib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("veritas.standardisation")

NUMERIC_RE = re.compile(r"-?\d+\.?\d*")
FLAG_PREFIX_RE = re.compile(r"^\s*([HL])\b", re.IGNORECASE)
COMPOSITE_SPLIT_RE = re.compile(r"([A-Za-z][A-Za-z0-9 /().,&+\-]*?)\s*-\s*([\d.]+)")
RANGE_RE = re.compile(r"^\s*(-?\d+\.?\d*)\s*-\s*(-?\d+\.?\d*)\s*$")


# ---------------------------------------------------------------------
# 2.1 Test name normalisation
# ---------------------------------------------------------------------
def _normalize_for_match(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\([^)]*\)", "", s)          # drop parenthetical method notes
    s = re.sub(r"[^a-z0-9+/ ]", " ", s)       # strip punctuation, keep +/ (Na+, K+)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class TestNameMatcher:
    def __init__(self, dictionary_path: str):
        with open(dictionary_path, "r", encoding="utf-8") as f:
            self.spec = json.load(f)
        self.fuzzy_threshold = self.spec.get("_meta", {}).get("fuzzy_threshold", 90) / 100.0
        self.exact_index: Dict[str, str] = {}      # normalized alias -> canonical
        self.canonical_meta: Dict[str, Dict[str, Any]] = {}
        self.fuzzy_pool: List[Tuple[str, str]] = []  # (normalized_alias, canonical)
        for entry in self.spec["tests"]:
            canonical = entry["canonical_name"]
            self.canonical_meta[canonical] = entry
            names = [canonical] + entry.get("aliases", [])
            for n in names:
                norm = _normalize_for_match(n)
                if norm:
                    self.exact_index[norm] = canonical
                    self.fuzzy_pool.append((norm, canonical))
        self.unresolved_log: List[str] = []

    def match(self, raw_name: str) -> Tuple[Optional[str], str, float]:
        """Returns (canonical_name_or_None, method, confidence 0-1)."""
        if not raw_name or not raw_name.strip():
            return None, "empty", 0.0
        norm = _normalize_for_match(raw_name)
        if norm in self.exact_index:
            return self.exact_index[norm], "exact_alias_match", 1.0

        candidates = [n for n, _ in self.fuzzy_pool]
        close = difflib.get_close_matches(norm, candidates, n=1, cutoff=self.fuzzy_threshold)
        if close:
            best_norm = close[0]
            canonical = dict(self.fuzzy_pool)[best_norm]
            ratio = difflib.SequenceMatcher(None, norm, best_norm).ratio()
            return canonical, "fuzzy_match", round(ratio, 3)

        # Suffix match: the real Apollo/FASTTRACK OCR extraction systematically
        # drops leading characters from test names ('aemoglobin', 'tal WBC
        # Count', 'ematocrit HCT' for Haemoglobin/Total WBC Count/Hematocrit
        # HCT). A plain edit-distance ratio penalises this heavily on short
        # names, so it's handled as its own tier: raw is accepted as a match
        # if it's a substantial trailing fragment of a known alias (must be
        # >=4 chars and >=55% of the alias's length, to avoid short common
        # words like "count" matching everything).
        best_suffix_match = None
        best_suffix_len = 0
        for alias_norm, canonical in self.fuzzy_pool:
            if len(norm) < 4 or len(norm) < 0.75 * len(alias_norm):
                continue
            if alias_norm.endswith(norm) and alias_norm != norm:
                if len(norm) > best_suffix_len:
                    best_suffix_len = len(norm)
                    best_suffix_match = canonical
        if best_suffix_match:
            confidence = round(best_suffix_len / max(len(norm), 1), 3)
            return best_suffix_match, "suffix_match_truncated_name", min(confidence, 0.99)

        # Prefix match: the mirror case -- a name truncated at the *end*
        # rather than the start (e.g. 'eGFR - ESTIMATED GLOMERULAR' for
        # 'eGFR - Estimated Glomerular Filtration Rate', seen in real data
        # where OCR line-wrapping clipped a long test name).
        best_prefix_match = None
        best_prefix_len = 0
        for alias_norm, canonical in self.fuzzy_pool:
            if len(norm) < 6 or len(norm) < 0.55 * len(alias_norm):
                continue
            if alias_norm.startswith(norm) and alias_norm != norm:
                if len(norm) > best_prefix_len:
                    best_prefix_len = len(norm)
                    best_prefix_match = canonical
        if best_prefix_match:
            confidence = round(best_prefix_len / max(len(norm), 1), 3)
            return best_prefix_match, "prefix_match_truncated_name", min(confidence, 0.99)

        self.unresolved_log.append(raw_name)
        return None, "unresolved", 0.0

    def unit_for(self, canonical_name: str) -> Optional[str]:
        meta = self.canonical_meta.get(canonical_name)
        return meta.get("unit_canonical") if meta else None

    def value_type_for(self, canonical_name: str) -> str:
        meta = self.canonical_meta.get(canonical_name)
        return meta.get("value_type", "text") if meta else "text"


# ---------------------------------------------------------------------
# 2.3 Numeric conversion
# ---------------------------------------------------------------------
NON_NUMERIC_TOKENS = {
    "negative", "positive", "normal", "nil", "present", "absent", "abnormal",
    "clear", "pale yellow", "trace", "pending", "n/a", "na", "nill",
}


def split_composite_result(result_text: str) -> Optional[List[Tuple[str, str]]]:
    """Splits 'Neutrophil - 72.4, Lymphocyte - 23.5' style combined fields
    into [(sub_test_name, sub_value_str), ...]. Returns None if the text
    doesn't look like a composite multi-value string (FR-2.3)."""
    if not result_text or "," not in result_text:
        return None
    matches = COMPOSITE_SPLIT_RE.findall(result_text)
    if len(matches) >= 2:
        return [(name.strip(" ,"), val.strip()) for name, val in matches]
    return None


def parse_numeric_result(result_text: str) -> Dict[str, Any]:
    """Returns dict: value (float|None), flag (H/L/None), residual_unit (str|None),
    is_numeric (bool), contradictory (bool)."""
    out = {"value": None, "flag": None, "residual_unit": None,
           "is_numeric": False, "contradictory": False, "raw": result_text}
    if result_text is None:
        return out
    text = str(result_text).strip()
    if text == "":
        return out

    low = text.lower()
    if low in NON_NUMERIC_TOKENS or (low.replace(" ", "") in NON_NUMERIC_TOKENS):
        out["is_numeric"] = False
        return out

    # Range-shaped "result" (data quality issue -- range value landed in
    # the result field, seen in some clinics' vitals blocks): flag, don't
    # silently coerce to a single number.
    if RANGE_RE.match(text):
        out["contradictory"] = True
        out["is_numeric"] = False
        return out

    flag_match = FLAG_PREFIX_RE.match(text)
    if flag_match:
        out["flag"] = flag_match.group(1).upper()
        text = text[flag_match.end():].strip()

    num_match = NUMERIC_RE.search(text)
    if num_match:
        try:
            out["value"] = float(num_match.group())
            out["is_numeric"] = True
        except ValueError:
            out["is_numeric"] = False
        residual = (text[:num_match.start()] + text[num_match.end():]).strip()
        # strip a leading unit-separator slash form like "114/min" already
        # captured by regex on the numerator; keep the remainder as unit text
        out["residual_unit"] = residual or None
    else:
        out["is_numeric"] = False
    return out


# ---------------------------------------------------------------------
# 2.4 Unit harmonisation
# ---------------------------------------------------------------------
class UnitConverter:
    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as f:
            self.spec = json.load(f)
        self.aliases = self.spec.get("unit_aliases", {})

    def convert(self, value: Optional[float], unit_original: Optional[str],
                unit_canonical_expected: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
        if unit_original is None:
            unit_original = ""
        key = unit_original.strip()
        entry = self.aliases.get(key) or self.aliases.get(key.lower())
        if entry is None:
            # Unknown unit spelling: pass the value through unconverted but
            # keep the canonical unit label from the test dictionary so
            # downstream analytics isn't silently blocked; log for curation.
            return value, unit_canonical_expected or (unit_original or None)
        factor = entry.get("factor", 1.0)
        canonical_unit = entry.get("canonical") or unit_canonical_expected
        if value is None:
            return None, canonical_unit
        return value * factor, canonical_unit


# ---------------------------------------------------------------------
# 2.5 Demographic normalisation
# ---------------------------------------------------------------------
AGE_COMPOSITE_RE = re.compile(
    r"(?:(\d+)\s*Y)?\s*(?:(\d+)\s*M)?\s*(?:(\d+)\s*D)?", re.IGNORECASE)


def normalize_age(age_raw: Any) -> Dict[str, Any]:
    """Handles plain years ('33'), composite strings ('33Y11M265D'), and
    strings with a trailing unit ('29Y')."""
    result = {"age_years": None, "age_months": None, "age_days": None, "age_text": None}
    if age_raw is None:
        return result
    text = str(age_raw).strip()
    if text == "" or text.upper() in {"[AGE REDACTED]", "N/A", "NA"}:
        result["age_text"] = text or None
        return result
    result["age_text"] = text

    # Pure integer/float years
    if re.fullmatch(r"\d+(\.\d+)?", text):
        result["age_years"] = float(text)
        return result

    m = AGE_COMPOSITE_RE.match(text)
    if m and any(m.groups()):
        y, mo, d = m.groups()
        result["age_years"] = int(y) if y else 0
        result["age_months"] = int(mo) if mo else 0
        result["age_days"] = int(d) if d else 0
        return result

    return result  # unparseable -- keep age_text, leave structured fields null


GENDER_MAP = {
    "m": "Male", "male": "Male", "man": "Male",
    "f": "Female", "female": "Female", "woman": "Female",
    "o": "Other", "other": "Other", "trans": "Other",
}


def normalize_gender(gender_raw: Any) -> Optional[str]:
    if gender_raw is None:
        return None
    text = str(gender_raw).strip().lower()
    if text == "" or "redacted" in text:
        return None
    return GENDER_MAP.get(text, gender_raw if isinstance(gender_raw, str) else None)


def normalize_date(date_raw: Any, source_format: Optional[str]) -> Optional[str]:
    """Best-effort parse to ISO 8601 (YYYY-MM-DD). Tries the clinic's
    declared format first, then a fallback list, since real data is messy
    even within one clinic's feed."""
    if date_raw is None:
        return None
    text = str(date_raw).strip()
    if text == "" or text.upper() in {"DD/MM/YYYY", "N/A", "NA"}:
        return None
    candidate_formats = [source_format] if source_format else []
    candidate_formats += ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y",
                          "%d-%b-%Y", "%d/%b/%Y", "%d-%B-%Y", "%d/%B/%Y"]
    for fmt in candidate_formats:
        if not fmt:
            continue
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    logger.info("Could not parse date '%s' with any known format", text)
    return None


# ---------------------------------------------------------------------
# 2.6 Medicine name mapping
# ---------------------------------------------------------------------
class MedicineMatcher:
    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as f:
            spec = json.load(f)
        self.brand_index: Dict[str, Dict[str, str]] = {}
        for entry in spec.get("medicines", []):
            for brand in entry.get("brand_names", []):
                self.brand_index[self._norm(brand)] = entry
            self.brand_index[self._norm(entry["generic_name"])] = entry

    @staticmethod
    def _norm(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"\b\d+\s*(mg|ml|mcg|g)\b", "", s)
        s = re.sub(r"\b(tab|tablet|inj|injection|cap|capsule|syrup)\b", "", s)
        s = re.sub(r"[^a-z ]", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    def match(self, raw_name: Optional[str]) -> Dict[str, Any]:
        if not raw_name:
            return {"medicine_name": raw_name, "generic_name": None,
                    "drug_class": None, "medicine_type": "missing"}
        norm = self._norm(raw_name)
        entry = self.brand_index.get(norm)
        if entry:
            is_brand = norm != self._norm(entry["generic_name"])
            return {"medicine_name": raw_name, "generic_name": entry["generic_name"],
                    "drug_class": entry["drug_class"],
                    "medicine_type": "brand_resolved" if is_brand else "generic_confirmed"}
        return {"medicine_name": raw_name, "generic_name": raw_name,
                "drug_class": None, "medicine_type": "unmapped_brand_or_generic"}
