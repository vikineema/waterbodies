import json
import logging
import os
from itertools import chain

import click
from datacube import Datacube

from waterbodies.db import get_waterbodies_engine
from waterbodies.io import check_directory_exists, get_filesystem
from waterbodies.logs import logging_setup
from waterbodies.surface_area_change import (  # noqa F401
    add_waterbody_observations_to_db,
    check_task_exists,
    get_waterbody_observations,
)
from waterbodies.text import get_task_id_str_from_tuple
from waterbodies.utils import TaskFailedError


@click.command(
    name="process-tasks",
    help="Process a list of tasks to generate waterbody observations.",
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
@click.option(
    "--tasks-list-file",
    type=str,
    help="Path to the text file containing the list of tasks to process.",
)
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
def process_tasks(
    verbose,
    run_type,
    tasks_list_file,
    historical_extent_rasters_directory,
    overwrite,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(path=historical_extent_rasters_directory):
        e = FileNotFoundError(f"Directory {historical_extent_rasters_directory} does not exist!")
        _log.error(e)
        raise e

    dc = Datacube(app=run_type)

    engine = get_waterbodies_engine()

    fs = get_filesystem(path=tasks_list_file, anon=True)
    with fs.open(tasks_list_file) as file:
        content = file.read()
        decoded_content = content.decode()
        tasks = json.loads(decoded_content)

    # In case file contains list of lists
    if all(isinstance(item, list) for item in tasks):
        tasks = list(chain(*tasks))
    else:
        pass

    failed_tasks = []
    for idx, task in enumerate(tasks):
        _log.info(f"Processing task: {task}   {idx+1}/{len(tasks)}")

        solar_day = task["solar_day"]
        tile_index_x = task["tile_index_x"]
        tile_index_y = task["tile_index_y"]
        task_datasets_ids = task["task_datasets_ids"]

        task_id_tuple = (solar_day, tile_index_x, tile_index_y)
        task_id_str = get_task_id_str_from_tuple(task_id_tuple)

        try:
            if run_type == "backlog-processing":
                if not overwrite:
                    exists = check_task_exists(task_id_str=task_id_str, engine=engine)

                if overwrite or not exists:
                    waterbody_observations = get_waterbody_observations(
                        solar_day=solar_day,
                        tile_index_x=tile_index_x,
                        tile_index_y=tile_index_y,
                        task_datasets_ids=task_datasets_ids,
                        historical_extent_rasters_directory=historical_extent_rasters_directory,
                        dc=dc,
                    )
                    if waterbody_observations is None:
                        _log.info(f"Task {task_id_str} has no waterbody observations")
                    else:
                        add_waterbody_observations_to_db(
                            waterbody_observations=waterbody_observations,
                            engine=engine,
                            update_rows=True,
                        )

                        _log.info(f"Task {task_id_str} complete")
                else:
                    _log.info(f"Task {task_id_str} already exists, skipping")

            elif run_type == "gap-filling":
                waterbody_observations = get_waterbody_observations(
                    solar_day=solar_day,
                    tile_index_x=tile_index_x,
                    tile_index_y=tile_index_y,
                    task_datasets_ids=task_datasets_ids,
                    historical_extent_rasters_directory=historical_extent_rasters_directory,
                    dc=dc,
                )
                if waterbody_observations is None:
                    _log.info(f"Task {task_id_str} has no waterbody observations")
                else:
                    add_waterbody_observations_to_db(
                        waterbody_observations=waterbody_observations,
                        engine=engine,
                        update_rows=True,
                    )

                    _log.info(f"Task {task_id_str} complete")
        except Exception as error:
            _log.exception(error)
            _log.error(f"Failed to process task {task}")
            failed_tasks.append(task)

    if failed_tasks:
        failed_tasks_json_array = json.dumps(failed_tasks)

        tasks_directory = "/tmp/"
        failed_tasks_output_file = os.path.join(tasks_directory, "failed_tasks")

        fs = get_filesystem(path=tasks_directory, anon=False)

        if not check_directory_exists(path=tasks_directory):
            fs.mkdirs(path=tasks_directory, exist_ok=True)
            _log.info(f"Created directory {tasks_directory}")

        with fs.open(failed_tasks_output_file, "a") as file:
            file.write(failed_tasks_json_array + "\n")
        _log.info(f"Failed tasks written to {failed_tasks_output_file}")

        raise TaskFailedError(f"{len(failed_tasks)} tasks failed")
