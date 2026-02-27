"""FUSE operations for the USNAN filesystem."""

from __future__ import annotations

import errno
import logging
import os
import stat
import time
from typing import Optional

from fuse import FuseOSError, Operations

from .cache import DownloadCache
from .catalog import Category, CategoryKey, DatasetCatalog

logger = logging.getLogger(__name__)


class NMRHubFS(Operations):
    """Read-only FUSE filesystem that presents NMRHub datasets as directories.

    Path hierarchy for most categories::

        /                               → list categories
        /<category>/                    → list dataset folders
        /<category>/<dataset>/          → triggers download, lists files
        /<category>/<dataset>/<path>    → triggers download, reads file

    For Published Datasets, there is an extra version level::

        /Published Datasets/<dataset>/          → list version subfolders
        /Published Datasets/<dataset>/<version>/ → triggers download, lists files
        /Published Datasets/<dataset>/<version>/<path> → reads file
    """

    def __init__(self, client, catalog: DatasetCatalog, cache: DownloadCache):
        self._client = client
        self._catalog = catalog
        self._cache = cache
        self._mount_time = time.time()
        # Map of open file descriptors → OS file descriptors
        self._open_fds: dict[int, int] = {}
        self._next_fh = 1

    # ------------------------------------------------------------------
    # Path parsing
    # ------------------------------------------------------------------

    _SENTINEL = object()

    def _parse_path(self, path: str):
        """Parse a FUSE path into its components.

        Returns a tuple ``(category, dataset_name, subpath)``.

        * For the root path ``/``, all three are ``None``.
        * For an unrecognised top-level name, *category* is the sentinel
          ``_SENTINEL`` (which will not match any ``Category`` member and
          will therefore be rejected by the FUSE operations).
        """
        parts = path.strip("/").split("/") if path != "/" else []

        category = None
        dataset_name = None
        subpath = None

        if len(parts) >= 1:
            category = self._resolve_category(parts[0])
            if category is None:
                # Path has a top-level component that isn't a known category.
                category = self._SENTINEL
        if len(parts) >= 2:
            dataset_name = parts[1]
        if len(parts) >= 3:
            subpath = "/".join(parts[2:])

        return category, dataset_name, subpath

    def _resolve_category(self, name: str) -> Optional[CategoryKey]:
        for cat in Category:
            if cat.value == name:
                return cat
        # Check custom directory names
        if name in self._catalog.available_categories():
            return name
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dir_stat(self, mtime: Optional[float] = None) -> dict:
        t = mtime or self._mount_time
        return {
            "st_mode": stat.S_IFDIR | 0o555,
            "st_nlink": 2,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
            "st_size": 0,
            "st_atime": t,
            "st_mtime": t,
            "st_ctime": t,
        }

    @staticmethod
    def _dir_stat_from_path(real_path) -> dict:
        st = os.stat(real_path)
        return {
            "st_mode": stat.S_IFDIR | 0o555,
            "st_nlink": st.st_nlink,
            "st_uid": st.st_uid,
            "st_gid": st.st_gid,
            "st_size": st.st_size,
            "st_atime": st.st_atime,
            "st_mtime": st.st_mtime,
            "st_ctime": st.st_ctime,
        }

    @staticmethod
    def _file_stat_from_path(real_path) -> dict:
        st = os.stat(real_path)
        return {
            "st_mode": stat.S_IFREG | 0o444,
            "st_nlink": 1,
            "st_uid": st.st_uid,
            "st_gid": st.st_gid,
            "st_size": st.st_size,
            "st_blocks": st.st_blocks,
            "st_blksize": st.st_blksize,
            "st_atime": st.st_atime,
            "st_mtime": st.st_mtime,
            "st_ctime": st.st_ctime,
        }

    def _resolve_dataset_id(self, category: CategoryKey, dataset_name: str) -> int:
        """Resolve a dataset folder name to its ID.

        Raises ``FuseOSError(ENOENT)`` if not found.  Does NOT download.
        """
        ds = self._catalog.get_dataset_by_folder_name(category, dataset_name)
        if ds is None:
            raise FuseOSError(errno.ENOENT)
        return ds.id

    def _resolve_version_id(self, dataset_name: str, version_label: str) -> int:
        """Resolve a PUBLISHED version label to a dataset ID.

        Raises ``FuseOSError(ENOENT)`` if not found.  Does NOT download.
        """
        listing = self._catalog.get_listing(Category.PUBLISHED)
        versions = listing.version_map.get(dataset_name)
        if versions is None or version_label not in versions:
            raise FuseOSError(errno.ENOENT)
        return versions[version_label]

    def _download(self, dataset_id: int) -> None:
        """Ensure a dataset is downloaded (called only from ``open()``)."""
        self._cache.ensure_downloaded(self._client, dataset_id)

    # ------------------------------------------------------------------
    # FUSE operations — read-only
    # ------------------------------------------------------------------

    def getattr(self, path, fh=None):
        category, dataset_name, subpath = self._parse_path(path)

        # Root
        if category is None:
            return self._dir_stat()

        # Validate category
        if category not in self._catalog.available_categories():
            raise FuseOSError(errno.ENOENT)

        # /<category>
        if dataset_name is None:
            return self._dir_stat()

        # /<category>/<dataset> — do NOT trigger download
        listing = self._catalog.get_listing(category)
        if dataset_name not in listing.folder_lookup:
            raise FuseOSError(errno.ENOENT)

        if subpath is None:
            mtime = listing.timestamps.get(dataset_name, self._mount_time)
            return self._dir_stat(mtime)

        # --- PUBLISHED: extra version directory level ---
        if category == Category.PUBLISHED:
            parts = subpath.split("/", 1)
            version_label = parts[0]
            inner_path = parts[1] if len(parts) > 1 else None

            # Validate version label exists (no download yet)
            versions = listing.version_map.get(dataset_name, {})
            if version_label not in versions:
                raise FuseOSError(errno.ENOENT)

            if inner_path is None:
                # /<category>/<dataset>/<version> — dir stat, no download
                return self._dir_stat(
                    listing.timestamps.get(dataset_name, self._mount_time)
                )

            # /<category>/<dataset>/<version>/<inner_path>
            # Only stat if already cached — don't trigger download
            ds_id = self._resolve_version_id(dataset_name, version_label)
            real = self._cache.resolve_path(ds_id, inner_path)
            if real is None:
                raise FuseOSError(errno.ENOENT)
            if real.is_dir():
                return self._dir_stat_from_path(real)
            return self._file_stat_from_path(real)

        # --- Standard categories ---
        # Only stat if already cached — don't trigger download
        ds_id = self._resolve_dataset_id(category, dataset_name)
        real = self._cache.resolve_path(ds_id, subpath)
        if real is None:
            raise FuseOSError(errno.ENOENT)

        if real.is_dir():
            return self._dir_stat(os.path.getmtime(real))
        return self._file_stat_from_path(real)

    def readdir(self, path, fh):
        category, dataset_name, subpath = self._parse_path(path)
        entries = [".", ".."]

        # Root: list available categories
        if category is None:
            for cat in self._catalog.available_categories():
                entries.append(self._catalog.category_display_name(cat))
            return entries

        if category not in self._catalog.available_categories():
            raise FuseOSError(errno.ENOENT)

        # /<category>: list dataset folders
        if dataset_name is None:
            listing = self._catalog.get_listing(category)
            entries.extend(listing.folder_lookup.keys())
            return entries

        # --- PUBLISHED: extra version directory level ---
        if category == Category.PUBLISHED:
            listing = self._catalog.get_listing(category)
            if dataset_name not in listing.folder_lookup:
                raise FuseOSError(errno.ENOENT)

            versions = listing.version_map.get(dataset_name, {})

            if subpath is None:
                # /<category>/<dataset>/ → list version subfolders
                entries.extend(sorted(versions.keys()))
                return entries

            # /<category>/<dataset>/<version>[/<inner>]
            # Triggers download on first access
            parts = subpath.split("/", 1)
            version_label = parts[0]
            inner_path = parts[1] if len(parts) > 1 else ""

            ds_id = self._resolve_version_id(dataset_name, version_label)
            self._download(ds_id)
            children = self._cache.list_entries(ds_id, inner_path)
            for child in children:
                entries.append(child.name)
            return entries

        # --- Standard categories ---
        # Triggers download on first access
        ds_id = self._resolve_dataset_id(category, dataset_name)
        self._download(ds_id)
        children = self._cache.list_entries(ds_id, subpath or "")
        for child in children:
            entries.append(child.name)
        return entries

    def open(self, path, flags):
        category, dataset_name, subpath = self._parse_path(path)

        if category is None or dataset_name is None or subpath is None:
            raise FuseOSError(errno.EISDIR)

        if category not in self._catalog.available_categories():
            raise FuseOSError(errno.ENOENT)

        # --- PUBLISHED: resolve version + inner path ---
        if category == Category.PUBLISHED:
            parts = subpath.split("/", 1)
            version_label = parts[0]
            inner_path = parts[1] if len(parts) > 1 else None

            if inner_path is None:
                # Trying to open a version dir as a file
                raise FuseOSError(errno.EISDIR)

            ds_id = self._resolve_version_id(dataset_name, version_label)
            self._download(ds_id)
            real = self._cache.resolve_path(ds_id, inner_path)
            if real is None or real.is_dir():
                raise FuseOSError(errno.ENOENT)
        else:
            # --- Standard categories ---
            ds_id = self._resolve_dataset_id(category, dataset_name)
            self._download(ds_id)
            real = self._cache.resolve_path(ds_id, subpath)
            if real is None or real.is_dir():
                raise FuseOSError(errno.ENOENT)

        os_fd = os.open(str(real), os.O_RDONLY)
        fh = self._next_fh
        self._next_fh += 1
        self._open_fds[fh] = os_fd
        return fh

    def read(self, path, size, offset, fh):
        os_fd = self._open_fds.get(fh)
        if os_fd is None:
            raise FuseOSError(errno.EBADF)
        os.lseek(os_fd, offset, os.SEEK_SET)
        return os.read(os_fd, size)

    def release(self, path, fh):
        os_fd = self._open_fds.pop(fh, None)
        if os_fd is not None:
            os.close(os_fd)

    def readlink(self, path):
        raise FuseOSError(errno.ENOENT)

    def statfs(self, path):
        return {
            "f_bsize": 4096,
            "f_frsize": 4096,
            "f_blocks": 0,
            "f_bfree": 0,
            "f_bavail": 0,
            "f_files": 0,
            "f_ffree": 0,
            "f_favail": 0,
            "f_namemax": 255,
        }

    # ------------------------------------------------------------------
    # Write operations — all return EROFS (Read-only file system)
    # ------------------------------------------------------------------

    def chmod(self, path, mode):
        raise FuseOSError(errno.EROFS)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EROFS)

    def create(self, path, mode, fi=None):
        raise FuseOSError(errno.EROFS)

    def link(self, target, source):
        raise FuseOSError(errno.EROFS)

    def mkdir(self, path, mode):
        raise FuseOSError(errno.EROFS)

    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EROFS)

    def rename(self, old, new):
        raise FuseOSError(errno.EROFS)

    def rmdir(self, path):
        raise FuseOSError(errno.EROFS)

    def symlink(self, target, source):
        raise FuseOSError(errno.EROFS)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EROFS)

    def unlink(self, path):
        raise FuseOSError(errno.EROFS)

    def utimens(self, path, times=None):
        raise FuseOSError(errno.EROFS)

    def write(self, path, data, offset, fh):
        raise FuseOSError(errno.EROFS)
