import logging
from datetime import datetime

import click
from datacube import Datacube
from odc.stats.model import DateTimeRange

from waterbodies.db import get_waterbodies_engine
from waterbodies.hopper import create_tasks_from_scenes
from waterbodies.io import check_directory_exists, find_geotiff_files
from waterbodies.logs import logging_setup
from waterbodies.surface_area_change import get_last_waterbody_observation_date
from waterbodies.text import parse_tile_id_from_filename


@click.command(name="generate-tiles", help="Generate tiles to run.", no_args_is_help=True)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--temporal-range",
    type=str,
    help=(
        "Only extract datasets for a given time range," "Example '2020-05--P1M' month of May 2020"
    ),
)
@click.option(
    "--run-type",
    type=click.Choice(["gap-filling", "backlog-processing", "regular-update"], case_sensitive=True),
)
@click.option(
    "--historical-extent-rasters-directory",
    type=str,
    help="Path to the directory containing the historical extent raster files.",
)
def generate_tiles(
    verbose,
    temporal_range,
    run_type,
    historical_extent_rasters_directory,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if run_type != "regular-update":
        temporal_range_ = DateTimeRange(temporal_range)

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
        parse_tile_id_from_filename(file_path=raster_file)
        for raster_file in historical_extent_rasters
    ]

    product = "wofs_ls"

    if run_type == "backlog-processing":
        dc = Datacube(app="backlog-processing")
        dc_query = dict(product=product, time=(temporal_range_.start, temporal_range_.end))
        # Search the datacube for all wofs_ls datasets whose acquisition times fall within
        # the temporal range specified.
        scenes = dc.find_datasets(**dc_query)
        tasks = create_tasks_from_scenes(scenes=scenes, tile_ids_of_interest=tile_ids_of_interest)

    elif run_type == "regular-update":
        # Connect to the waterbodies engine
        engine = get_waterbodies_engine()
        # TODO: Check if this should be done here or should the time range be defined outside
        # this step then passed as to temporal-range parameter for this step
        # Get the date of the most recent waterbody observation
        last_observation_date = get_last_waterbody_observation_date(engine=engine)
        today = datetime.now()

        dc = Datacube(app="regular-update")
        dc_query = dict(product=product, time=(last_observation_date, today))
        # Search the datacube for all wofs_ls datasets whose acquisition times fall within
        # the temporal range specified.
        scenes = dc.find_datasets(**dc_query)
        tasks = create_tasks_from_scenes(scenes=scenes, tile_ids_of_interest=tile_ids_of_interest)

    elif run_type == "gap-filling":
        dc = Datacube(app="gap-filling")
        # The difference between gap-filling and the other steps is here
        # we are searching for datasets by their creation date (`creation_time`),
        # not their acquisition date (`time`).
        dc_query = dict(product=product, creation_time=(temporal_range_.start, temporal_range_.end))
        # Search the datacube for all wofs_ls datasets whose creation times (not acquisition time)
        # fall within the temporal range specified.
        # E.g  a dataset can have an aquisition date of 2023-12-15 but have been added to the
        # datacube in 2024-02, which will be its creation date.
        scenes = dc.find_datasets(**dc_query)
        # Identify the tasks / waterbody observations affected.
        affected_tasks = create_tasks_from_scenes(
            scenes=scenes, tile_ids_of_interest=tile_ids_of_interest
        )
        # Get the ids for the affected waterbody observations.
        task_ids = [
            task_id for task in affected_tasks for task_id, task_dataset_ids in task.items()
        ]
        # For each task id add an empty list as a place holder for the task datasets ids.
        # This will be filled in the drill function which is easier to do in parallel than
        # looping over each task to update the required task dataset ids here.
        tasks = [{task_id: []} for task_id in task_ids]

    return tasks
