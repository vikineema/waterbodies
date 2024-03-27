import datetime
import logging
from itertools import chain

from datacube import Datacube
from datacube.model import Dataset
from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table
from tqdm import tqdm

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.hopper import (
    create_tasks_from_scenes,
    find_datasets_by_creation_date,
    find_datasets_by_task_id,
)

_log = logging.getLogger(__name__)


def create_waterbodies_observations_table(engine: Engine) -> Table:
    """
    Create the waterbodies_observations table if it does not exist.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    Table
        The waterbodies_observations table.
    """
    table = create_table(engine=engine, db_model=WaterbodyObservation)
    return table


def get_last_waterbody_observation_date(engine: Engine) -> datetime:
    """
    Get the date of the last waterbody observation.

    Parametersrom odc.dscache.tools.tiling import parse_gridspec_with_name
    from odc.dscache.tools import bin_dataset_stream
    from waterbodies.hopper import persist     Date of the last waterbody observation.
    ----------
    engine : Engine | None

    Returns
    -------
    datetime
        Date of the last waterbody observation
    """
    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(engine)
    with Session.begin() as session:
        last_observation_date = session.query(func.max(table.c.date)).scalar()

    return last_observation_date


def create_tasks_for_gapfill_run(
    engine: Engine,
    tile_ids_of_interest: [list[tuple[int]]],
) -> list[dict[tuple[str, int, int], list[str]]]:
    """
    List all the tasks required for a gap fill run.

    Parameters
    ----------
    engine : Engine
    tile_ids_of_interest : list[tuple[int]]]
        Tile ids to filter to.

    Returns
    -------
    list[dict[tuple[str, int, int], list[str]]]
        Tasks to run for the a gap fill run
    """

    last_observation_date = get_last_waterbody_observation_date(engine=engine).date()

    today = datetime.datetime.now().date()

    dc = Datacube(app="gapfill")

    gapfill_scenes = find_datasets_by_creation_date(
        product="wofs_ls", start_date=last_observation_date, end_date=today, dc=dc
    )

    _log.info(
        f"Found {len(gapfill_scenes)} new wofs_ls scenes added "
        f'to the datacube since {last_observation_date.strftime("%Y-%m-%d")}'
    )

    # Find all the task ids affected by the gapfill scenes.
    gapfill_scenes_tasks = create_tasks_from_scenes(
        scenes=gapfill_scenes, tile_ids_of_interest=tile_ids_of_interest
    )
    gapfill_scenes_task_ids = [
        task_id for task in gapfill_scenes_tasks for task_id, task_datasets in task.items()
    ]
    _log.found(f"Found {len(gapfill_scenes_task_ids)} affected by the gap fill scenes")

    # For each task id find the scenes overlapping it.
    scenes = []
    with tqdm(
        iterable=gapfill_scenes_task_ids,
        desc=f"Getting scenes for {len(gapfill_scenes_task_ids)} task ids",
        total=len(gapfill_scenes_task_ids),
    ) as gapfill_scenes_task_ids:
        for task_id in gapfill_scenes_task_ids:
            scenes.append(find_datasets_by_task_id(task_id=task_id, dc=dc, product="wofs_ls"))

    scenes = list(chain.from_iterable(scenes))
    _log.found(f"{len(scenes)} scenes found overlappng the gapfill tasks")

    gapfill_scenes_tile_ids = [(task_id[1], task_id[2]) for task_id in gapfill_scenes_task_ids]
    tasks = create_tasks_from_scenes(scenes=scenes, tile_ids_of_interest=gapfill_scenes_tile_ids)
    return tasks
