import json
import logging
import sys

import click
from datacube import Datacube
from odc.stats.model import DateTimeRange

from waterbodies.hopper import create_tasks_from_scenes
from waterbodies.io import check_directory_exists, find_geotiff_files
from waterbodies.logs import logging_setup
from waterbodies.text import format_task, get_tile_id_tuple_from_filename


@click.command(name="generate-tasks", help="Generate tasks to run.", no_args_is_help=True)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--temporal-range",
    type=str,
    help=(
        "Only extract datasets for a given time range, e.g. '2020-05--P1M' month of May 2020. "
        "For backlog-processing this will query datasets by their acquisition time. "
        "For gap-filling this will query datasets by their creation time."
    ),
)
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
    "--historical-extent-rasters-directory",
    type=str,
    help="Path to the directory containing the historical extent raster files.",
)
def generate_tasks(
    verbose,
    temporal_range,
    run_type,
    historical_extent_rasters_directory,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(path=historical_extent_rasters_directory):
        e = FileNotFoundError(f"Directory {historical_extent_rasters_directory} does not exist!")
        _log.error(e)
        raise e
    else:
        historical_extent_rasters = find_geotiff_files(
            directory_path=historical_extent_rasters_directory
        )

    # Get the tile_ids for tiles that actually contain waterbodies.
    tile_ids_of_interest = [
        get_tile_id_tuple_from_filename(file_path=raster_file)
        for raster_file in historical_extent_rasters
    ]

    product = "wofs_ls"

    temporal_range_ = DateTimeRange(temporal_range)

    dc = Datacube(app=run_type)

    if run_type == "backlog-processing":
        dc_query = dict(product=product, time=(temporal_range_.start, temporal_range_.end))
        # Search the datacube for all wofs_ls datasets whose acquisition times fall within
        # the temporal range specified.
        scenes = dc.find_datasets(**dc_query)
        tasks = create_tasks_from_scenes(scenes=scenes, tile_ids_of_interest=tile_ids_of_interest)

    elif run_type == "gap-filling":
        # The difference between gap-filling and the bcklog-processing is here
        # we are searching for datasets by their creation date (`creation_time`),
        # not their acquisition date (`time`).
        dc_query = dict(product=product, creation_time=(temporal_range_.start, temporal_range_.end))
        # Search the datacube for all wofs_ls datasets whose creation times (not acquisition time)
        # fall within the temporal range specified.
        # E.g  a dataset can have an aquisition date of 2023-12-15 but have been added to the
        # datacube in 2024-02, which will be its creation date.
        scenes = dc.find_datasets(**dc_query)
        # Get the ids of the tasks to process.
        tasks_ = create_tasks_from_scenes(scenes=scenes, tile_ids_of_interest=tile_ids_of_interest)
        task_ids = [task_id for task in tasks_ for task_id, task_dataset_ids in task.items()]
        # For each task id add an empty list as a place holder for the task's datasets' ids
        # which will be filled  during processing. This is because the processing step is expected
        # to be done in parallel hence filling the task's datasets' ids will be faster there than
        # looping over each task to update the required datasets' ids here.
        tasks = [{task_id: []} for task_id in task_ids]

    # Put the tasks in the correct format.
    tasks = [format_task(task) for task in tasks]
    json.dump(tasks, sys.stdout)
