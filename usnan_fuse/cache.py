"""On-disk download cache for dataset files."""

from __future__ import annotations

import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import List, Optional, Union

from platformdirs import user_cache_dir

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path(user_cache_dir("usnan-fuse", ensure_exists=True))
_DEFAULT_MAX_BYTES = 4 * 1024 * 1024 * 1024  # 4 GB


class DownloadCache:
    """Thread-safe, on-disk cache of downloaded dataset contents.

    Each dataset is stored under ``<cache_dir>/<dataset_id>/``.  When the
    total cache size exceeds *max_bytes*, the least-recently-accessed
    datasets are evicted until the cache is back under the limit.
    """

    def __init__(
        self,
        cache_dir: Union[str, Path, None] = None,
        max_bytes: int = _DEFAULT_MAX_BYTES,
    ):
        self._root = Path(cache_dir) if cache_dir else _DEFAULT_CACHE_DIR
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        # Per-dataset locks prevent concurrent downloads of the same dataset.
        self._locks: dict[int, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def _lock_for(self, dataset_id: int) -> threading.Lock:
        with self._global_lock:
            if dataset_id not in self._locks:
                self._locks[dataset_id] = threading.Lock()
            return self._locks[dataset_id]

    def _dataset_dir(self, dataset_id: int) -> Path:
        return self._root / str(dataset_id)

    def is_cached(self, dataset_id: int) -> bool:
        d = self._dataset_dir(dataset_id)
        return d.is_dir() and any(d.iterdir())

    def cached_size(self, dataset_id: int) -> Optional[int]:
        """Return total size in bytes of a cached dataset, or None if not cached."""
        d = self._dataset_dir(dataset_id)
        if not d.is_dir() or not any(d.iterdir()):
            return None
        return self._dir_size(d)

    def ensure_downloaded(self, client, dataset_id: int) -> Path:
        """Download the dataset if not already cached. Thread-safe.

        Returns the path to the cached dataset directory.
        """
        lock = self._lock_for(dataset_id)
        with lock:
            ds_dir = self._dataset_dir(dataset_id)
            if ds_dir.is_dir() and any(ds_dir.iterdir()):
                # Touch the directory so LRU tracking stays current
                ds_dir.touch()
                return ds_dir

            # Clean up any partial previous download
            if ds_dir.exists():
                shutil.rmtree(ds_dir)

            # Download into a temp directory on the same filesystem, then
            # atomically rename into place.  This prevents a half-extracted
            # directory from being mistaken for a complete cache entry if
            # the process is killed mid-download.
            logger.info("Downloading dataset %d …", dataset_id)
            tmp_dir = Path(tempfile.mkdtemp(
                prefix=f".dl-{dataset_id}-", dir=self._root
            ))
            try:
                client.datasets.download([dataset_id], tmp_dir)
                self._unwrap(tmp_dir)
                tmp_dir.rename(ds_dir)
            except Exception:
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                raise
            logger.info("Dataset %d cached at %s", dataset_id, ds_dir)

        # Evict outside the per-dataset lock to avoid holding it during IO
        self._evict_if_needed()
        return ds_dir

    def list_entries(self, dataset_id: int, subpath: str = "") -> List[Path]:
        """List immediate children of a (sub)directory inside a cached dataset."""
        ds_dir = self._dataset_dir(dataset_id)
        target = ds_dir / subpath if subpath else ds_dir
        if not target.is_dir():
            return []
        return sorted(target.iterdir())

    def resolve_path(self, dataset_id: int, subpath: str) -> Optional[Path]:
        """Resolve a subpath inside a cached dataset to a real filesystem path.

        Returns ``None`` if the path does not exist.
        """
        ds_dir = self._dataset_dir(dataset_id)
        full = (ds_dir / subpath).resolve()
        # Prevent path traversal
        if not str(full).startswith(str(ds_dir.resolve())):
            return None
        return full if full.exists() else None

    # ------------------------------------------------------------------
    # Post-download unwrapping
    # ------------------------------------------------------------------

    @staticmethod
    def _unwrap(ds_dir: Path) -> None:
        """Flatten the ZIP download structure.

        The SDK downloads a ZIP containing ``experiments.csv`` and a single
        experiment subfolder.  We remove the CSV and move the subfolder's
        contents up so the user sees the experiment data directly.
        """
        # Remove experiments.csv (the per-download manifest)
        csv = ds_dir / "experiments.csv"
        if csv.exists():
            csv.unlink()

        # Find the single remaining subdirectory
        subdirs = [p for p in ds_dir.iterdir() if p.is_dir()]
        if len(subdirs) != 1:
            return  # unexpected structure — leave as-is

        experiment_dir = subdirs[0]
        # Move all contents up into ds_dir
        for child in list(experiment_dir.iterdir()):
            child.rename(ds_dir / child.name)
        experiment_dir.rmdir()

    # ------------------------------------------------------------------
    # Size-based eviction
    # ------------------------------------------------------------------

    @staticmethod
    def _dir_size(path: Path) -> int:
        """Total size of all files under *path* in bytes."""
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    def _evict_if_needed(self) -> None:
        """Remove least-recently-accessed dataset dirs until under max_bytes."""
        with self._global_lock:
            # Collect (mtime, size, path) for each cached dataset directory
            entries = []
            for child in self._root.iterdir():
                if not child.is_dir():
                    continue
                try:
                    int(child.name)
                except ValueError:
                    continue  # skip non-dataset dirs
                size = self._dir_size(child)
                mtime = child.stat().st_mtime
                entries.append((mtime, size, child))

            total = sum(size for _, size, _ in entries)
            if total <= self._max_bytes:
                return

            # Sort oldest first
            entries.sort(key=lambda e: e[0])
            for mtime, size, path in entries:
                if total <= self._max_bytes:
                    break
                logger.info(
                    "Evicting cached dataset %s (%.1f MB) to stay under limit",
                    path.name,
                    size / (1024 * 1024),
                )
                shutil.rmtree(path)
                total -= size
