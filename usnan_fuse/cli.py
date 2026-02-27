"""CLI entry point for the USNAN FUSE filesystem."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from fuse import FUSE
from usnan import USNANClient

from .cache import DownloadCache
from .catalog import DatasetCatalog
from .fs import NMRHubFS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="usnan-fuse",
        description="Mount NMRHub datasets as a FUSE filesystem.",
    )
    parser.add_argument("mountpoint", help="Directory to mount the filesystem on.")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Cache directory for downloaded datasets (default: ~/.cache/usnan-fuse/).",
    )
    parser.add_argument(
        "--ttl",
        type=float,
        default=300,
        help="Listing cache TTL in seconds (default: 300).",
    )
    parser.add_argument(
        "--max-cache-size",
        type=float,
        default=4.0,
        help="Maximum download cache size in GB (default: 4.0).",
    )
    parser.add_argument(
        "-f", "--foreground",
        action="store_true",
        help="Run in foreground (don't daemonize).",
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug logging (implies --foreground).",
    )
    parser.add_argument(
        "--api-url",
        default="https://api.nmrhub.org",
        help="NMRHub API base URL.",
    )
    parser.add_argument(
        "--login",
        choices=["browser", "device"],
        default=None,
        help="Trigger an interactive login flow before mounting.",
    )
    parser.add_argument(
        "--custom-dirs",
        default=None,
        metavar="FILE",
        help="Path to a JSON file defining custom directories with search filters.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.debug:
        args.foreground = True

    # fusepy logs full tracebacks for every ENOENT at DEBUG level
    # (normal OS probes like .Trash, .xdg-volume-info, autorun.inf).
    # Keep it at INFO to suppress those.
    logging.getLogger("fuse").setLevel(logging.INFO)

    # Client
    client = USNANClient(base_url=args.api_url)

    # Only trigger interactive login if --login was requested AND no
    # session was already restored from disk by the client constructor.
    if args.login and not (client._auth and client._auth.authenticated):
        client.login(method=args.login)

    # Components
    max_bytes = int(args.max_cache_size * 1024 * 1024 * 1024)
    catalog = DatasetCatalog(client, ttl=args.ttl)

    # Register custom directories from JSON config
    if args.custom_dirs:
        try:
            with open(args.custom_dirs) as f:
                custom_dirs = json.load(f)
            for entry in custom_dirs:
                catalog.add_custom_directory(entry["name"], entry["filters"])
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            parser.error(f"Invalid custom-dirs JSON: {e}")
        except FileNotFoundError:
            parser.error(f"Custom dirs file not found: {args.custom_dirs}")

    cache = DownloadCache(cache_dir=args.cache_dir, max_bytes=max_bytes)
    fs = NMRHubFS(client, catalog, cache)

    auth = getattr(client, "_auth", None)
    if auth and auth.authenticated:
        logging.getLogger(__name__).info("Authenticated — private categories available.")
    else:
        logging.getLogger(__name__).info("Unauthenticated — only public datasets visible.")

    logging.getLogger(__name__).info("Mounting on %s", args.mountpoint)

    FUSE(fs, args.mountpoint, foreground=args.foreground, ro=True, nothreads=False)
