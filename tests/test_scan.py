import click
from click.testing import CliRunner
from datasette.app import Datasette
from datasette.cli import cli
import pytest


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
