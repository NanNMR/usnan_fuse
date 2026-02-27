"""Utility helpers for the USNAN FUSE filesystem."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def deduplicate_names(datasets) -> Dict[int, str]:
    """Map dataset IDs to unique display names for use as folder names.

    When two datasets share the same ``dataset_name`` within a category,
    the experiment start time is appended to disambiguate (e.g.
    ``"my_dataset (2024-06-15 10:30)"``).  If timestamps also collide,
    the dataset ID is appended as a last resort.

    Slashes and null bytes are replaced with underscores so names are safe
    for use as path components.
    """
    # Build a list of (dataset, sanitized_name) pairs
    entries: List[tuple] = []
    for ds in datasets:
        raw_name = getattr(ds, "dataset_name", None) or f"dataset_{ds.id}"
        entries.append((ds, _sanitize(raw_name)))

    # Group by sanitized name
    name_groups: Dict[str, list] = {}
    for ds, safe_name in entries:
        name_groups.setdefault(safe_name, []).append(ds)

    result: Dict[int, str] = {}
    for name, group in name_groups.items():
        if len(group) == 1:
            result[group[0].id] = name
            continue

        # Collision — append formatted experiment_start_time
        timestamped: Dict[str, list] = {}
        for ds in group:
            ts_str = _format_timestamp(getattr(ds, "experiment_start_time", None))
            display = f"{name} ({ts_str})" if ts_str else name
            timestamped.setdefault(display, []).append(ds)

        for display, sub_group in timestamped.items():
            if len(sub_group) == 1:
                result[sub_group[0].id] = display
            else:
                # Timestamps also collide — fall back to ID
                for ds in sub_group:
                    result[ds.id] = f"{display} [{ds.id}]"

    return result


def _format_timestamp(iso_str: Optional[str]) -> Optional[str]:
    """Format an ISO timestamp as ``YYYY-MM-DD HH:MM`` for display in folder names."""
    if not iso_str:
        return None
    try:
        cleaned = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return None


_UNSAFE_RE = re.compile(r"[/\x00]")


def _sanitize(name: str) -> str:
    """Replace characters that are invalid in POSIX filenames."""
    return _UNSAFE_RE.sub("_", name).strip()


def parse_iso_timestamp(iso_str: Optional[str]) -> float:
    """Parse an ISO-8601 datetime string to a UNIX epoch float.

    Returns 0.0 if the string is ``None`` or unparseable.
    """
    if not iso_str:
        return 0.0
    try:
        # Python 3.11+ handles trailing Z; for earlier versions strip it.
        cleaned = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return 0.0
