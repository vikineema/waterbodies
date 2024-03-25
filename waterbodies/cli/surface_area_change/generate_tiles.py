import click
import logging
from waterbodies.logs import logging_setup
from waterbodies.db import get_dev_waterbodies_engine, get_existing_table_names


@click.command(name="generate-tiles", help="Generate tiles to run.", no_args_is_help=True)
@click.option("-v", "--verbose", default=1, count=True)
@click.option("--env",
              default="PROD",
              type=click.Choice(["PROD", "DEV"]),
              case_sensitive=True)
def generate_tiles(
    verbose,
    env,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    engine = get_wta
