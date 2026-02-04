from datasette import hookimpl
import click
import json
import os
import subprocess

import sqlite_scanner


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
        if directories:
            found = scan_directories(directories)
            db_files.extend(found)

        # Delegate to serve with the discovered files
        ctx = click.get_current_context()
        ctx.invoke(serve_cmd, files=tuple(db_files), **kwargs)

    # Copy all options from serve to scan (but not arguments)
    if serve_cmd:
        for param in serve_cmd.params:
            if isinstance(param, click.Option):
                scan.params.append(param)
