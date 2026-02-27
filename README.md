# usnan-fuse

A read-only FUSE filesystem for browsing and accessing [NMRHub](https://nmrhub.org) datasets as local files. Datasets are downloaded on demand and cached locally with LRU eviction.

## Installation

Requires Python 3.10+.

```bash
pip install .
```

## Usage

Mount the filesystem to a directory:

```bash
mkdir -p /mnt/nmrhub
usnan-fuse /mnt/nmrhub
```

Then browse datasets with standard tools:

```bash
ls /mnt/nmrhub/
# Public Datasets/  Published Datasets/

ls "/mnt/nmrhub/Public Datasets/"
# dataset-folder-1/  dataset-folder-2/  ...

cat "/mnt/nmrhub/Public Datasets/some-dataset/acqu"
```

To unmount:

```bash
fusermount -u /mnt/nmrhub
```

### Options

| Option | Default | Description |
|---|---|---|
| `--cache-dir PATH` | `~/.cache/usnan-fuse/` | Cache directory for downloaded datasets |
| `--ttl SECONDS` | 300 | Listing cache TTL |
| `--max-cache-size GB` | 4.0 | Max download cache size before LRU eviction |
| `-f, --foreground` | | Run in foreground |
| `-d, --debug` | | Enable debug logging (implies `--foreground`) |
| `--api-url URL` | `https://api.nmrhub.org` | NMRhubAPI base URL |
| `--login {browser,device}` | | Trigger interactive login for private datasets |
| `--custom-dirs FILE` | | JSON file defining custom directory filters |

### Authentication

By default only public and published datasets are visible. To access private datasets (lab group, personal), log in first:

```bash
usnan-fuse /mnt/nmrhub --login browser
```

### Custom Directories

Define additional folders via a JSON file:

```json
[
  {
    "name": "Project Datasets",
    "filters": [
      {
        "field": "_perm_reason",
        "value": "project",
        "match_mode": "array-includes",
        "operator": "OR"
      }
    ]
  }
]
```

```bash
usnan-fuse /mnt/nmrhub --custom-dirs my-filters.json
```

## Dependencies

- [usnan](https://pypi.org/project/usnan/) - NMRhubclient SDK
- [fusepy](https://pypi.org/project/fusepy/) - Python FUSE bindings
- [platformdirs](https://pypi.org/project/platformdirs/) - Cross-platform directory resolution
