from datasette import hookimpl
import click


@hookimpl
def register_commands(cli):
    serve_cmd = cli.commands.get("serve")

    @cli.command()
    @click.argument("paths", type=click.Path(), nargs=-1)
    def scan(paths, **kwargs):
        """Scan directories for SQLite files and serve them with Datasette"""
        pass

    # Copy all options from serve to scan (but not arguments)
    if serve_cmd:
        for param in serve_cmd.params:
            if isinstance(param, click.Option):
                scan.params.append(param)
