"""Dataset catalog — fetches and caches dataset listings per category."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from usnan.models.search import SearchConfig

from .filters import RawSearchConfig
from .utils import deduplicate_names, parse_iso_timestamp

logger = logging.getLogger(__name__)

# A category identifier is either a built-in Category enum member or a
# custom directory name (str).
CategoryKey = Union["Category", str]


class Category(Enum):
    PUBLIC = "Public Datasets"
    PUBLISHED = "Published Datasets"
    LAB_GROUP = "Lab Group Datasets"
    MY_DATASETS = "My Datasets"


@dataclass
class CachedListing:
    """Snapshot of datasets for a single category."""

    datasets: list = field(default_factory=list)
    # dataset_id → display folder name
    name_map: Dict[int, str] = field(default_factory=dict)
    # display folder name → dataset
    folder_lookup: dict = field(default_factory=dict)
    # display folder name → epoch timestamp (for stat)
    timestamps: Dict[str, float] = field(default_factory=dict)
    # (PUBLISHED only) folder name → {version_label: dataset_id}
    version_map: Dict[str, Dict[str, int]] = field(default_factory=dict)
    fetched_at: float = 0.0


@dataclass
class CustomDirectory:
    """A user-defined directory with custom search filters."""
    name: str
    filters: List[Dict[str, Any]]


class DatasetCatalog:
    """Fetches and caches dataset listings with a configurable TTL."""

    def __init__(self, client, ttl: float = 300.0):
        self._client = client
        self._ttl = ttl
        self._cache: Dict[CategoryKey, CachedListing] = {}
        self._custom_dirs: Dict[str, CustomDirectory] = {}

    def add_custom_directory(self, name: str, filters: List[Dict[str, Any]]) -> None:
        """Register a custom directory with the given search filters."""
        self._custom_dirs[name] = CustomDirectory(name=name, filters=filters)
        logger.info("Registered custom directory: %s", name)

    def available_categories(self) -> List[CategoryKey]:
        """Return the categories visible given the current auth state."""
        auth = getattr(self._client, "_auth", None)
        if auth and auth.authenticated:
            cats: List[CategoryKey] = [
                Category.PUBLIC,
                Category.PUBLISHED,
                Category.LAB_GROUP,
                Category.MY_DATASETS,
            ]
        else:
            cats = [Category.PUBLIC, Category.PUBLISHED]

        # Append custom directories
        cats.extend(self._custom_dirs.keys())
        return cats

    def category_display_name(self, category: CategoryKey) -> str:
        """Return the display name for a category (used as folder name)."""
        if isinstance(category, Category):
            return category.value
        return category  # custom dirs use their name directly

    def get_listing(self, category: CategoryKey) -> CachedListing:
        """Return a (possibly cached) listing for *category*."""
        cached = self._cache.get(category)
        if cached and (time.time() - cached.fetched_at) < self._ttl:
            return cached

        display = self.category_display_name(category)
        logger.info("Refreshing listing for %s", display)
        datasets = list(self._fetch(category))
        name_map = deduplicate_names(datasets)

        folder_lookup = {}
        timestamps: Dict[str, float] = {}
        version_map: Dict[str, Dict[str, int]] = {}

        for ds in datasets:
            folder_name = name_map[ds.id]
            folder_lookup[folder_name] = ds
            timestamps[folder_name] = parse_iso_timestamp(
                getattr(ds, "experiment_start_time", None)
            )

            # For PUBLISHED, build the version subfolder mapping
            if category == Category.PUBLISHED:
                versions: Dict[str, int] = {"Original": ds.id}
                if ds.versions:
                    for v in ds.versions:
                        versions[f"v{v.version}"] = v.dataset.id
                version_map[folder_name] = versions

        listing = CachedListing(
            datasets=datasets,
            name_map=name_map,
            folder_lookup=folder_lookup,
            timestamps=timestamps,
            version_map=version_map,
            fetched_at=time.time(),
        )
        self._cache[category] = listing
        return listing

    def get_dataset_by_folder_name(self, category: CategoryKey, name: str):
        """Look up a Dataset object by its display folder name."""
        listing = self.get_listing(category)
        return listing.folder_lookup.get(name)

    # ------------------------------------------------------------------
    # Internal: build search configs per category
    # ------------------------------------------------------------------

    def _fetch(self, category: CategoryKey):
        """Generator that yields Dataset objects for *category*."""
        config = self._build_config(category)
        if config is None:
            return
        yield from self._client.datasets.search(config)

    def _build_config(self, category: CategoryKey):
        if category == Category.PUBLIC:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return SearchConfig(records=10000).add_filter(
                "public_time", value=now, match_mode="lessThan"
            )

        if category == Category.PUBLISHED:
            # Fetch original datasets (version=None) that have published versions
            cfg = RawSearchConfig(records=10000)
            cfg.add_raw_filter(
                "_has_published_version", value=True, match_mode="equals"
            )
            cfg.add_raw_filter("version", value=True, match_mode="isNull")
            return cfg

        if category == Category.LAB_GROUP:
            cfg = RawSearchConfig(records=10000)
            cfg.add_raw_filter(
                "_perm_reason", value="project", match_mode="array-includes", operator="OR"
            )
            cfg.add_raw_filter(
                "_perm_reason", value="lab-group", match_mode="array-includes", operator="OR"
            )
            cfg.add_raw_filter(
                "_perm_reason", value="own-data", match_mode="array-includes", operator="OR"
            )
            return cfg

        if category == Category.MY_DATASETS:
            cfg = RawSearchConfig(records=10000)
            cfg.add_raw_filter(
                "_perm_reason", value="own-data", match_mode="array-includes"
            )
            return cfg

        # Custom directory
        if isinstance(category, str) and category in self._custom_dirs:
            custom = self._custom_dirs[category]
            cfg = RawSearchConfig(records=10000)
            for f in custom.filters:
                cfg.add_raw_filter(
                    f["field"],
                    value=f.get("value"),
                    match_mode=f.get("match_mode", "equals"),
                    operator=f.get("operator", "AND"),
                )
            return cfg

        return None
