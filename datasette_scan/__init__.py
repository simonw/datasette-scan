from datasette import hookimpl
from datasette.database import Database
import click
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time

import sqlite_scanner


def validate_databases(paths):
    """Check each path is a readable SQLite database.

    Returns (valid, skipped) where skipped is a list of (path, reason) tuples.
    """
    valid = []
    skipped = []
    for path in paths:
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                conn.execute("SELECT * FROM sqlite_master")
                valid.append(path)
            finally:
                conn.close()
        except Exception as e:
            skipped.append((path, str(e)))
    return valid, skipped


def scan_directories(directories):
    """Scan directories for SQLite files using the sqlite-scanner binary.

    Returns a list of absolute paths to discovered SQLite files.
    """
    binary = sqlite_scanner.get_binary_path()
    if not os.path.exists(binary):
        raise click.ClickException(
            f"sqlite-scanner binary not found at {binary}"
        )
    result = subprocess.run(
        [binary, "--jsonl"] + list(directories),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise click.ClickException(
            f"sqlite-scanner failed: {result.stderr.strip()}"
        )
    paths = []
    for line in result.stdout.strip().splitlines():
        if line:
            entry = json.loads(line)
            paths.append(entry["path"])
    return paths


def rescan_and_add_databases(ds, directories, known_paths):
    """Re-scan directories and add any newly discovered databases to Datasette.

    Returns the updated set of known paths.
    """
    found = scan_directories(directories)
    known_paths = set(known_paths)
    for path in found:
        if path not in known_paths:
            db = Database(ds, path=path, is_mutable=True)
            ds.add_database(db)
            known_paths.add(path)
    return known_paths


def _background_scanner(ds, directories, known_paths, interval):
    """Background thread that periodically re-scans for new databases."""
    while True:
        time.sleep(interval)
        try:
            known_paths = rescan_and_add_databases(ds, directories, known_paths)
        except Exception as e:
            click.echo(f"Scan error: {e}", err=True)


@hookimpl
def register_commands(cli):
    serve_cmd = cli.commands.get("serve")

    @cli.command()
    @click.argument("paths", type=click.Path(), nargs=-1)
    @click.option(
        "--scan-interval",
        type=float,
        default=None,
        help="Re-scan directories for new databases every N seconds",
    )
    def scan(paths, scan_interval, **kwargs):
        """Scan directories for SQLite files and serve them with Datasette"""
        if not paths:
            paths = (".",)

        # Separate files from directories
        db_files = []
        directories = []
        for p in paths:
            if os.path.isdir(p):
                directories.append(p)
            else:
                db_files.append(p)

        # Scan directories for SQLite files
        scanned_files = []
        if directories:
            scanned_files = scan_directories(directories)

        # Validate scanned files, skip corrupted ones
        if scanned_files:
            valid, skipped = validate_databases(scanned_files)
            for path, reason in skipped:
                click.echo(
                    f"Skipping {path}: {reason}", err=True
                )
            db_files.extend(valid)

        # Always use nolock for safety with discovered files
        kwargs["nolock"] = True

        if scan_interval is not None and directories:
            # Continuous scanning mode: build and run the server ourselves
            # so we can add databases to the running instance
            known_paths = set(db_files)
            ds = serve_cmd.callback(
                files=tuple(db_files), return_instance=True, **kwargs
            )
            # Start background scanner thread
            scanner_thread = threading.Thread(
                target=_background_scanner,
                args=(ds, directories, known_paths, scan_interval),
                daemon=True,
            )
            scanner_thread.start()

            # Handle --get: do the request and exit
            get = kwargs.get("get")
            if get:
                from datasette.utils.testing import TestClient

                client = TestClient(ds)
                response = client.get(get)
                click.echo(response.text)
                sys.exit(0 if response.status == 200 else 1)

            # Start the server
            import uvicorn

            host = kwargs.get("host", "127.0.0.1")
            port = kwargs.get("port", 8001)
            uvicorn_kwargs = dict(
                host=host,
                port=port,
                log_level="info",
                lifespan="on",
                workers=1,
            )
            uds = kwargs.get("uds")
            if uds:
                uvicorn_kwargs["uds"] = uds
            ssl_keyfile = kwargs.get("ssl_keyfile")
            if ssl_keyfile:
                uvicorn_kwargs["ssl_keyfile"] = ssl_keyfile
            ssl_certfile = kwargs.get("ssl_certfile")
            if ssl_certfile:
                uvicorn_kwargs["ssl_certfile"] = ssl_certfile
            uvicorn.run(ds.app(), **uvicorn_kwargs)
        else:
            # Simple mode: delegate to serve
            ctx = click.get_current_context()
            ctx.invoke(serve_cmd, files=tuple(db_files), **kwargs)

    # Copy all options from serve to scan (but not arguments),
    # excluding --nolock which is always enabled for scan
    if serve_cmd:
        for param in serve_cmd.params:
            if isinstance(param, click.Option) and param.name != "nolock":
                scan.params.append(param)
