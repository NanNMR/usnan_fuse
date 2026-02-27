"""RawSearchConfig — duck-types SearchConfig for filters the SDK doesn't support.

The SDK's SearchConfig validates field names against Dataset dataclass fields and
its MatchMode literal doesn't include 'array-includes'. Two filters we need are
unsupported:

- ``_perm_reason`` with ``matchMode: "array-includes"`` (not a Dataset field)
- ``person_id`` with ``matchMode: "equals"`` (not a Dataset field)

RawSearchConfig implements the same interface that DatasetsEndpoint.search() uses:
``clone()``, ``build()``, ``.offset``, and ``.records``.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List


class RawFilterMetadata:
    """A single raw filter entry."""

    __slots__ = ("value", "match_mode", "operator")

    def __init__(self, value: Any, match_mode: str = "equals", operator: str = "AND"):
        self.value = value
        self.match_mode = match_mode
        self.operator = operator

    def to_dict(self) -> Dict[str, Any]:
        return {"value": self.value, "matchMode": self.match_mode, "operator": self.operator}


class RawSearchConfig:
    """Search configuration that bypasses SDK field/match-mode validation.

    Duck-types ``SearchConfig`` so it can be passed to
    ``DatasetsEndpoint.search()`` without modifications to the SDK.
    """

    def __init__(
        self,
        records: int = 25,
        offset: int = 0,
        sort_order: str = "ASC",
        sort_field: str | None = None,
    ):
        self.records = records
        self.offset = offset
        self.sort_order = sort_order
        self.sort_field = sort_field
        self._filters: Dict[str, List[RawFilterMetadata]] = {}

    def add_raw_filter(
        self,
        field: str,
        *,
        value: Any,
        match_mode: str = "equals",
        operator: str = "AND",
    ) -> RawSearchConfig:
        """Add a filter without any field or match-mode validation.

        Returns self for method chaining.
        """
        entry = RawFilterMetadata(value=value, match_mode=match_mode, operator=operator)
        self._filters.setdefault(field, []).append(entry)
        return self

    def build(self) -> Dict[str, Any]:
        """Return a dict in the same shape as ``SearchConfig.build()``."""
        filters_dict = {
            key: [f.to_dict() for f in value] for key, value in self._filters.items()
        }
        return {
            "filters": json.dumps(filters_dict),
            "offset": self.offset,
            "sort_order": self.sort_order,
            "sort_field": self.sort_field,
            "records": self.records,
        }

    def clone(self) -> RawSearchConfig:
        """Return a shallow copy (same contract as ``SearchConfig.clone()``)."""
        new = RawSearchConfig(
            records=self.records,
            offset=self.offset,
            sort_order=self.sort_order,
            sort_field=self.sort_field,
        )
        new._filters = copy.deepcopy(self._filters)
        return new
