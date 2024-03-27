import datetime
import logging

from datacube import Datacube
from datacube.model import Dataset
from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.hopper import (
    create_task_from_task_id,
    create_tasks_from_scenes,
    find_datasets_by_creation_date,
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


def get_gapfill_scenes(engine: Engine, dc: Datacube) -> list[Dataset]:
    """
    Get all the wofs_ls scenes added to the datacube since the
    date of the last waterbody observation to today.

    Parameters
    ----------
    engine : Engine
    dc : Datacube

    Returns
    -------
    list[Dataset]
        All the wofs_ls scenes added to the datacube since the date of the
        last waterbody observation to today.
    """

    last_observation_date = get_last_waterbody_observation_date(engine=engine).date()

    today = datetime.datetime.now().date()

    dss = find_datasets_by_creation_date(
        product="wofs_ls", start_date=last_observation_date, end_date=today, dc=dc
    )

    _log.info(
        f"Found {len(dss)} new wofs_ls scenes added "
        f'to the datacube since {last_observation_date.strftime("%Y-%m-%d")}'
    )
    return dss


def get_task_ids_for_gapfill_scenes(
    engine: Engine,
    tile_ids_of_interest: [list[tuple[int]]],
    dc: Datacube,
) -> list[str]:
    """
    Get all the task ids for the tasks affected by
    all the wofs_ls scenes added to the datacube since the
    date of the last waterbody observation to today.

    Parameters
    ----------
    engine : Engine
    tile_ids_of_interest : list[tuple[int]]]
    dc : Datacube

    Returns
    -------
    list[str]
        Task ids for the gapfill scenes
    """
    # Find all the new wofs_ls scenes added to the datacube
    # since the last waterbody observation date.
    gapfill_scenes = get_gapfill_scenes(engine=engine, dc=dc)
    # Find all the task ids affected by the gapfill scenes.
    gapfill_scenes_tasks = create_tasks_from_scenes(
        scenes=gapfill_scenes, tile_ids_of_interest=tile_ids_of_interest
    )
    gapfill_scenes_task_ids = [
        task_id for task in gapfill_scenes_tasks for task_id, task_datasets in task.items()
    ]
    return gapfill_scenes_task_ids


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

    dc = Datacube(app="gapfill")

    gapfill_scenes_task_ids = get_task_ids_for_gapfill_scenes(
        engine=engine, tile_ids_of_interest=tile_ids_of_interest, dc=dc
    )
    # For each task find the datasets that overlap it
    tasks = []
    for task_id in gapfill_scenes_task_ids:
        tasks.append(create_task_from_task_id(task_id=task_id, dc=dc, product="wofs_ls"))

    return tasks
