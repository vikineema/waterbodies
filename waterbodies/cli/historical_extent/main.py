import click

from waterbodies.cli.historical_extent.generate_tasks import generate_tasks
from waterbodies.cli.historical_extent.process_polygons import process_polygons
from waterbodies.cli.historical_extent.process_tasks import process_tasks
from waterbodies.cli.historical_extent.rasterise_polygons import rasterise_polygons
from waterbodies.cli.historical_extent.split_hydrosheds_land_mask import (
    split_hydrosheds_land_mask,
)


@click.group(name="historical-extent", help="Run the waterbodies historical extent tools.")
def historical_extent():
    pass


historical_extent.add_command(rasterise_polygons)
historical_extent.add_command(generate_tasks)
historical_extent.add_command(process_tasks)
historical_extent.add_command(process_polygons)
historical_extent.add_command(split_hydrosheds_land_mask)
