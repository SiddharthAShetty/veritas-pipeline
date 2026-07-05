"""
Tiny dotted-path resolver used to read values out of arbitrarily-nested
clinic JSON according to a config-declared path, e.g. "data.basic_info.age"
or "meta.patientAge".

This is deliberately minimal (dict traversal only, no list indexing needed
by current clinic configs) -- it exists so that clinic onboarding is a
config change (add a path string) rather than a code change (NFR-2.1).
If a future clinic needs list-indexed paths, extend `get_path` rather than
writing clinic-specific Python.
"""
from typing import Any, Optional


def get_path(obj: Any, path: Optional[str], default: Any = None) -> Any:
    """Resolve a dotted path like 'data.basic_info.age' against nested dicts.
    Returns `default` if any segment is missing or the container isn't a dict.
    """
    if not path:
        return default
    current = obj
    for segment in path.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return default
    return current


def has_required_keys(obj: Any, keys: list) -> bool:
    """Used by clinic-config 'envelope_shape' matching / sanity checks."""
    if not isinstance(obj, dict):
        return False
    return all(k in obj for k in keys)
