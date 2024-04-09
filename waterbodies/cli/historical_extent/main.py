import click

from waterbodies.cli.historical_extent.rasterise_polygons import rasterise_polygons


@click.group(name="historical-extent", help="Run the waterbodies historical extent tools.")
def historical_extent():
    pass


historical_extent.add_command(rasterise_polygons)
