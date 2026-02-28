import json
import logging
import os
from itertools import chain

import click
from datacube import Datacube

from waterbodies.historical_extent import get_waterbodies
from waterbodies.io import check_directory_exists, check_file_exists, get_filesystem
from waterbodies.logs import logging_setup
from waterbodies.text import get_tile_index_str_from_tuple
from waterbodies.utils import TaskFailedError


@click.command(
    name="process-tasks",
    help="Process a list of tasks to generate waterbodies.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--tasks-list-file",
    type=str,
    help="Path to the text file containing the list of tasks to process.",
)
@click.option(
    "--land-sea-mask-rasters-directory",
    type=str,
    help="Path to the directory containing the rasters to use for masking ocean and sea pixels.",
)
@click.option(
    "--output-directory",
    type=str,
    help="Directory to write the waterbody polygons generated.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun tasks that have already been processed. ",
)
def process_tasks(
    verbose,
    tasks_list_file,
    land_sea_mask_rasters_directory,
    output_directory,
    overwrite,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dc = Datacube(app="process-tasks")

    min_polygon_size = 6
    max_polygon_size = 1000
    detection_threshold = 0.1
    extent_threshold = 0.05
    min_valid_observations = 60

    if not check_directory_exists(path=land_sea_mask_rasters_directory):
        e = FileNotFoundError(f"Directory {land_sea_mask_rasters_directory} does not exist!")
        _log.error(e)
        raise e

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

    if not check_directory_exists(path=output_directory):
        fs = get_filesystem(output_directory, anon=False)
        fs.mkdirs(output_directory)
        _log.info(f"Created the directory {output_directory}")

    failed_tasks = []
    for idx, task in enumerate(tasks):
        _log.info(f"Processing task: {task}   {idx+1}/{len(tasks)}")
        tile_index_x = task["tile_index_x"]
        tile_index_y = task["tile_index_y"]
        task_datasets_ids = task["task_datasets_ids"]

        task_id_tuple = (tile_index_x, tile_index_y)
        task_id_str = get_tile_index_str_from_tuple(task_id_tuple)
        output_file_name = os.path.join(output_directory, f"waterbodies_{task_id_str}.parquet")

        try:
            if not overwrite:
                exists = check_file_exists(output_file_name)

            if overwrite or not exists:
                waterbody_polygons = get_waterbodies(
                    tile_index_x=tile_index_x,
                    tile_index_y=tile_index_y,
                    task_datasets_ids=task_datasets_ids,
                    dc=dc,
                    land_sea_mask_rasters_directory=land_sea_mask_rasters_directory,
                    detection_threshold=detection_threshold,
                    extent_threshold=extent_threshold,
                    min_valid_observations=min_valid_observations,
                    min_polygon_size=min_polygon_size,
                    max_polygon_size=max_polygon_size,
                )
                if waterbody_polygons.empty:
                    _log.info(f"Task {task_id_str} has no waterbody polygons")
                else:
                    _log.info(
                        f"Task {task_id_str} has {len(waterbody_polygons)} waterbody polygons"
                    )
                    waterbody_polygons.to_parquet(output_file_name)
                    _log.info(f"Waterbodies written to {output_file_name}")
            else:
                _log.info(f"Task {task_id_str} already exists, skipping")
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
