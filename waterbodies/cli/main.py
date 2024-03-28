import click

from waterbodies.cli.surface_area_change.main import surface_area_change


@click.group(name="waterbodies", help="Run DE Africa's waterbodies tools")
def waterbodies():
    pass


waterbodies.add_command(surface_area_change)
