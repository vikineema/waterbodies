import logging

import click
from datacube import Datacube

from waterbodies.db import get_waterbodies_engine
from waterbodies.io import check_directory_exists
from waterbodies.logs import logging_setup
from waterbodies.surface_area_change import (
    add_waterbody_observations_to_db,
    check_if_task_exists,
    get_waterbody_observations,
)


@click.command(
    name="process-task",
    help="Process a single task to generate waterbody observations.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option("--task", type=dict, help="Task to process")
@click.option(
    "--historical-extent-rasters-directory",
    type=str,
    help="Path to the directory containing the historical extent raster files.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun tasks that have already been processed.",
)
def process_task(verbose, task, historical_extent_rasters_directory, overwrite):

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    engine = get_waterbodies_engine()

    dc = Datacube(app="ProcessTask")

    if not check_directory_exists(path=historical_extent_rasters_directory):
        e = FileNotFoundError(f"Directory {historical_extent_rasters_directory} does not exist!")
        _log.error(e)
        raise e

    if not overwrite:
        exists = check_if_task_exists(task=task, engine=engine)

    if overwrite or not exists:
        waterbody_observations = get_waterbody_observations(
            task=task,
            historical_extent_rasters_directory=historical_extent_rasters_directory,
            dc=dc,
        )
        add_waterbody_observations_to_db(
            waterbody_observations=waterbody_observations, engine=engine, update_rows=overwrite
        )
    else:
        _log.info(f"Task {task} already exists, skipping")
