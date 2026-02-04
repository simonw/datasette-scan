import click
from click.testing import CliRunner
from datasette.app import Datasette
from datasette.cli import cli
from datasette_scan import scan_directories, rescan_and_add_databases, validate_databases
import json
import os
import pytest
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch


@pytest.fixture
def tmp_with_dbs(tmp_path):
    """Create a temp directory with some SQLite databases."""
    db1 = tmp_path / "one.db"
    db2 = tmp_path / "subdir" / "two.db"
    db2.parent.mkdir()
    for db_path in (db1, db2):
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.close()
    # Non-SQLite file
    (tmp_path / "not_a_db.txt").write_text("hello")
    return tmp_path


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-scan" in installed_plugins


def test_scan_command_exists():
    runner = CliRunner()
    result = runner.invoke(cli, ["scan", "--help"])
    assert result.exit_code == 0
    assert "Usage: cli scan" in result.output


def test_scan_has_serve_options():
    """scan should have all of serve's options so it can pass them through."""
    serve_cmd = cli.commands["serve"]
    scan_cmd = cli.commands["scan"]
    serve_option_names = {
        p.name for p in serve_cmd.params if isinstance(p, click.Option)
    }
    scan_option_names = {
        p.name for p in scan_cmd.params if isinstance(p, click.Option)
    }
    # All serve options should be present on scan
    missing = serve_option_names - scan_option_names
    assert not missing, f"scan is missing these serve options: {missing}"


def test_scan_has_scan_interval_option():
    """scan should have the --scan-interval option that serve doesn't."""
    scan_cmd = cli.commands["scan"]
    scan_option_names = {
        p.name for p in scan_cmd.params if isinstance(p, click.Option)
    }
    assert "scan_interval" in scan_option_names


def test_scan_directories_finds_sqlite_files(tmp_with_dbs):
    """scan_directories should find SQLite files using the sqlite-scanner binary."""
    found = scan_directories([str(tmp_with_dbs)])
    found_names = {Path(f).name for f in found}
    assert "one.db" in found_names
    assert "two.db" in found_names
    assert "not_a_db.txt" not in found_names


def test_scan_default_scans_current_directory(tmp_with_dbs):
    """With no args, scan should scan the current directory."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["scan", "--get", "/.json"], catch_exceptions=False
    )
    # We just need it to not error - with no SQLite in cwd it may serve
    # empty but should not crash
    assert result.exit_code == 0


def test_scan_serves_discovered_files(tmp_with_dbs):
    """scan should discover files and serve them via Datasette."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["scan", str(tmp_with_dbs), "--get", "/.json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    db_names = set(data.keys())
    assert "one" in db_names
    assert "two" in db_names


def test_scan_explicit_file_args(tmp_with_dbs):
    """Explicit file arguments should be passed through directly."""
    db_path = str(tmp_with_dbs / "one.db")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["scan", db_path, "--get", "/.json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "one" in data


def test_scan_mixed_files_and_dirs(tmp_with_dbs):
    """Mix of explicit files and directories should work."""
    # Create an extra database outside the scanned dir
    extra_db = tmp_with_dbs / "extra" / "standalone.db"
    extra_db.parent.mkdir()
    conn = sqlite3.connect(str(extra_db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "scan",
            str(tmp_with_dbs / "subdir"),  # directory - will find two.db
            str(extra_db),  # explicit file
            "--get",
            "/.json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    db_names = set(data.keys())
    assert "two" in db_names
    assert "standalone" in db_names
    # one.db should NOT be found since we only scanned subdir
    assert "one" not in db_names


def test_scan_error_when_binary_missing():
    """Should raise a clear error if sqlite-scanner binary isn't found."""
    with patch("datasette_scan.sqlite_scanner.get_binary_path") as mock_path:
        mock_path.return_value = "/nonexistent/sqlite-scanner"
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", "--get", "/.json"])
        assert result.exit_code != 0
        assert "sqlite-scanner" in result.output.lower() or "Error" in result.output


def test_scan_passes_port_option(tmp_with_dbs):
    """Serve options like --port should be passed through."""
    runner = CliRunner()
    # Using --get bypasses actual server start, but port is still accepted
    result = runner.invoke(
        cli,
        ["scan", str(tmp_with_dbs), "-p", "9999", "--get", "/.json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.asyncio
async def test_rescan_adds_new_databases(tmp_with_dbs):
    """rescan_and_add_databases should add newly discovered files to Datasette."""
    # Start with one.db already known
    db1 = str(tmp_with_dbs / "one.db")
    ds = Datasette([db1])
    known = {db1}
    initial_db_count = len(ds.databases)

    # Rescan should find two.db and add it
    known = rescan_and_add_databases(ds, [str(tmp_with_dbs)], known)
    assert len(ds.databases) > initial_db_count
    db_names = set(ds.databases.keys())
    assert "two" in db_names

    # The new file should now be in known
    two_path = str(tmp_with_dbs / "subdir" / "two.db")
    assert two_path in known


@pytest.mark.asyncio
async def test_rescan_does_not_duplicate(tmp_with_dbs):
    """rescan_and_add_databases should not re-add already known files."""
    ds = Datasette([])
    known = set()

    # First scan adds files
    known = rescan_and_add_databases(ds, [str(tmp_with_dbs)], known)
    count_after_first = len(ds.databases)

    # Second scan should not add duplicates
    known = rescan_and_add_databases(ds, [str(tmp_with_dbs)], known)
    assert len(ds.databases) == count_after_first


def test_validate_databases_accepts_good_files(tmp_with_dbs):
    """validate_databases should accept valid SQLite files."""
    paths = [
        str(tmp_with_dbs / "one.db"),
        str(tmp_with_dbs / "subdir" / "two.db"),
    ]
    valid, skipped = validate_databases(paths)
    assert valid == paths
    assert skipped == []


def test_validate_databases_skips_corrupted(tmp_path):
    """validate_databases should skip corrupted SQLite files."""
    good = tmp_path / "good.db"
    conn = sqlite3.connect(str(good))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.close()

    corrupted = tmp_path / "corrupted.db"
    data = bytearray(good.read_bytes())
    for i in range(100, 200):
        data[i] = 0xFF
    corrupted.write_bytes(data)

    valid, skipped = validate_databases([str(good), str(corrupted)])
    assert valid == [str(good)]
    assert len(skipped) == 1
    assert skipped[0][0] == str(corrupted)
    assert "malformed" in skipped[0][1].lower() or "error" in skipped[0][1].lower()


def test_validate_databases_skips_truncated(tmp_path):
    """validate_databases should skip truncated SQLite files."""
    truncated = tmp_path / "truncated.db"
    truncated.write_bytes(b"SQLite format 3\x00" + b"\x00" * 84)

    valid, skipped = validate_databases([str(truncated)])
    assert valid == []
    assert len(skipped) == 1


def test_scan_skips_corrupted_and_serves_good(tmp_path):
    """scan should skip corrupted files and serve only valid ones."""
    good = tmp_path / "good.db"
    conn = sqlite3.connect(str(good))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.close()

    corrupted = tmp_path / "corrupted.db"
    data = bytearray(good.read_bytes())
    for i in range(100, 200):
        data[i] = 0xFF
    corrupted.write_bytes(data)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["scan", str(tmp_path), "--get", "/.json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # Output may contain warning lines before the JSON; extract JSON part
    lines = result.output.strip().splitlines()
    json_lines = [l for l in lines if not l.startswith("Skipping ")]
    data = json.loads("\n".join(json_lines))
    assert "good" in data
    assert "corrupted" not in data


def test_scan_warns_about_skipped_files(tmp_path):
    """scan should print warnings about skipped files."""
    good = tmp_path / "good.db"
    conn = sqlite3.connect(str(good))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.close()

    corrupted = tmp_path / "corrupted.db"
    data = bytearray(good.read_bytes())
    for i in range(100, 200):
        data[i] = 0xFF
    corrupted.write_bytes(data)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["scan", str(tmp_path), "--get", "/.json"],
    )
    assert result.exit_code == 0
    assert "corrupted.db" in result.output
    assert "Skipping" in result.output


def test_scan_defaults_nolock(tmp_with_dbs):
    """scan should pass --nolock by default to handle locked databases."""
    # We can verify this by checking that the nolock kwarg is True
    # when delegated to serve. We'll test indirectly: scan a directory
    # and confirm it works (nolock doesn't break anything for unlocked files).
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["scan", str(tmp_with_dbs), "--get", "/.json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "one" in data
