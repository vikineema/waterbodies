import json
import logging

import click
from datacube import Datacube

from waterbodies.db import get_waterbodies_engine
from waterbodies.hopper import find_task_datasets_ids
from waterbodies.io import check_directory_exists
from waterbodies.logs import logging_setup
from waterbodies.surface_area_change import (
    add_waterbody_observations_to_db,
    check_task_exists,
    get_waterbody_observations,
)
from waterbodies.text import get_task_id_str_from_tuple


@click.command(
    name="process-task",
    help="Process a single task to generate waterbody observations.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--run-type",
    default="backlog-processing",
    type=click.Choice(
        [
            "backlog-processing",
            "gap-filling",
        ],
        case_sensitive=True,
    ),
)
@click.option("--task", type=str, help="Task to process")
@click.option(
    "--historical-extent-rasters-directory",
    type=str,
    help="Path to the directory containing the historical extent raster files.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help=(
        "Rerun tasks that have already been processed. "
        "Overwrite is ignored if run type is gap-filling."
    ),
)
def process_task(
    verbose,
    run_type,
    task,
    historical_extent_rasters_directory,
    overwrite,
):

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(path=historical_extent_rasters_directory):
        e = FileNotFoundError(f"Directory {historical_extent_rasters_directory} does not exist!")
        _log.error(e)
        raise e

    product = "wofs_ls"

    dc = Datacube(app=run_type)

    engine = get_waterbodies_engine()

    _log.info(f"Processing task: {task}")
    task = json.loads(task)

    solar_day = task["solar_day"]
    tile_id_x = task["tile_id_x"]
    tile_id_y = task["tile_id_y"]
    task_datasets_ids = task["task_datasets_ids"]

    task_id_tuple = (solar_day, tile_id_x, tile_id_y)
    task_id_str = get_task_id_str_from_tuple(task_id_tuple)

    if run_type == "backlog-processing":

        if not overwrite:
            exists = check_task_exists(task_id_str=task_id_str, engine=engine)

        if overwrite or not exists:
            waterbody_observations = get_waterbody_observations(
                solar_day=solar_day,
                tile_id_x=tile_id_x,
                tile_id_y=tile_id_y,
                task_datasets_ids=task_datasets_ids,
                historical_extent_rasters_directory=historical_extent_rasters_directory,
                dc=dc,
            )
            if waterbody_observations is None:
                _log.info(f"Task {task_id_str} has no waterbody observations")
            else:
                add_waterbody_observations_to_db(
                    waterbody_observations=waterbody_observations, engine=engine, update_rows=True
                )
                _log.info(f"Task {task_id_str} complete")
        else:
            _log.info(f"Task {task_id_str} already exists, skipping")

    elif run_type == "gap-filling":
        # Find the dataset ids for the task.
        task_datasets_ids = find_task_datasets_ids(
            solar_day=solar_day, tile_id_x=tile_id_x, tile_id_y=tile_id_y, dc=dc, product=product
        )
        waterbody_observations = get_waterbody_observations(
            solar_day=solar_day,
            tile_id_x=tile_id_x,
            tile_id_y=tile_id_y,
            task_datasets_ids=task_datasets_ids,
            historical_extent_rasters_directory=historical_extent_rasters_directory,
            dc=dc,
        )
        if waterbody_observations is None:
            _log.info(f"Task {task_id_str} has no waterbody observations")
        else:
            add_waterbody_observations_to_db(
                waterbody_observations=waterbody_observations, engine=engine, update_rows=True
            )
            _log.info(f"Task {task_id_str} complete")
