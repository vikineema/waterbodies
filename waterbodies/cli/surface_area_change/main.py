import click

from waterbodies.cli.surface_area_change.generate_tasks import generate_tasks


@click.group(name="surface-area-change", help="Run the waterbodies surface area change tools.")
def surface_area_change():
    pass


surface_area_change.add_command(generate_tasks)
