# datasette-scan

[![PyPI](https://img.shields.io/pypi/v/datasette-scan.svg)](https://pypi.org/project/datasette-scan/)
[![Changelog](https://img.shields.io/github/v/release/simonw/datasette-scan?include_prereleases&label=changelog)](https://github.com/simonw/datasette-scan/releases)
[![Tests](https://github.com/simonw/datasette-scan/actions/workflows/test.yml/badge.svg)](https://github.com/simonw/datasette-scan/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-scan/blob/main/LICENSE)

Datasette plugin that scans directories for SQLite database files and serves them.

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-scan
```

## Usage

Scan the current directory for SQLite files and serve them:

```bash
datasette scan
```

Scan a specific directory:

```bash
datasette scan /path/to/directory
```

Scan multiple directories and include explicit database files:

```bash
datasette scan /data/projects /data/archives extra.db
```

Specify a port (or any other `datasette serve` option):

```bash
datasette scan -p 8001
```

All options accepted by `datasette serve` are also accepted by `datasette scan` -- these are introspected at runtime, so new options added to future versions of Datasette will be picked up automatically.

### How it works

- Positional arguments that are **directories** are scanned recursively for SQLite files using the [sqlite-scanner](https://pypi.org/project/sqlite-scanner/) binary, which identifies SQLite files by their magic bytes rather than file extension.
- Positional arguments that are **files** are passed through directly as database files.
- If no positional arguments are given, the current directory (`.`) is scanned.
- Discovered files are then served as if you had run `datasette serve file1.db file2.db ...`.

### Examples

Scan the current directory and open in a browser:

```bash
datasette scan -o
```

Scan with CORS enabled on a custom port:

```bash
datasette scan /data --cors -p 9000
```

Use `--get` to test without starting a server:

```bash
datasette scan /data --get /.json
```

## Development

To set up this plugin locally, first checkout the code. You can confirm it is available like this:
```bash
cd datasette-scan
# Confirm the plugin is visible
uv run datasette plugins
```
To run the tests:
```bash
uv run pytest
```
