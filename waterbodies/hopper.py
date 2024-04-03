import datetime
import logging
from itertools import chain
from types import SimpleNamespace

import toolz
from datacube import Datacube
from datacube.model import Dataset
from odc.dscache.tools import bin_dataset_stream
from odc.stats.tasks import CompressedDataset, compress_ds
from odc.stats.utils import Cell
from tqdm import tqdm

from waterbodies.grid import WaterbodiesGrid

dt_range = SimpleNamespace(start=None, end=None)
_log = logging.getLogger(__name__)


# From https://github.com/opendatacube/odc-stats/blob/develop/odc/stats/tasks.py
def update_start_end(x: datetime, out: SimpleNamespace):
    """
    Add a start and end datetime to a CompressedDataset.

    Parameters
    ----------
    x : datetime
        Datetime or date range to use.
    out : SimpleNamespace
        An empty simplenamespace object to fill.
    """
    if out.start is None:
        out.start = x
        out.end = x
    else:
        out.start = min(out.start, x)
        out.end = max(out.end, x)


def persist(ds: Dataset) -> CompressedDataset:
    """
    Mapping function to use when binning a dataset stream.
    """
    # Convert the dataset to a CompressedDataset.
    _ds = compress_ds(ds)
    # Add a start and end datetime to the CompressedDataset.
    update_start_end(_ds.time, dt_range)
    return _ds


def bin_by_solar_day(cells: dict[tuple[int, int], Cell]) -> dict[tuple[str, int, int], list[str]]:
    """
    Bin by solar day.

    Parameters
    ----------
    cells : dict[tuple[int, int], Cell]
        Cells to bin.

    Returns
    -------
    dict[tuple[str, int, int], list[str]]
        Tasks containing the task id (solar_day, tile id x, tile id y) and Datasets UUIDs
        for the task.
    """
    tasks = {}
    for tile_id, cell in cells.items():
        utc_offset = cell.utc_offset
        grouped_by_solar_day = toolz.groupby(lambda ds: (ds.time + utc_offset).date(), cell.dss)

        for solar_day_date, dss in grouped_by_solar_day.items():
            solar_day_key = (solar_day_date.strftime("%Y-%m-%d"),)
            tasks[solar_day_key + tile_id] = dss

    # Remove duplicate source uids
    # Duplicates occur when queried datasets are captured around UTC midnight
    # and around weekly boundary
    # From the compressed datasets keep only the dataset uuids
    tasks = {task_id: [ds.id for ds in set(dss)] for task_id, dss in tasks.items()}
    return tasks


def create_tasks_from_scenes(
    scenes: list[Dataset], tile_ids_of_interest: list[tuple[int]] = []
) -> list[dict[tuple[str, int, int], list[str]]]:
    """
    Create tasks to run from scenes.

    Parameters
    ----------
    scenes : list[Dataset]
        Scenes to  create tasks for.
    tile_ids_of_interest : list[tuple[int]], optional
        Tile ids of interest, by default []

    Returns
    -------
    list[dict[tuple[str, int, int], list[str]]]
        A list of tasks with each tasks containing the task id (solar_day, tile id x, tile id y)
        and Datasets UUID
    """

    cells = {}

    dss = bin_dataset_stream(
        gridspec=WaterbodiesGrid().gridspec, dss=scenes, cells=cells, persist=persist
    )

    with tqdm(iterable=dss, desc=f"Processing {len(scenes):8,d} scenes", total=len(scenes)) as dss:
        for _ in dss:
            pass

    if tile_ids_of_interest:
        _log.info(
            f"Filter the {len(cells)} cells to keep only the cells "
            f"containing the {len(tile_ids_of_interest)} tile ids of interest."
        )
        cells = {
            tile_id: cell for tile_id, cell in cells.items() if tile_id in tile_ids_of_interest
        }
        _log.info(f"Total number of cells after filtering: {len(cells)}")
    else:
        _log.info(f"Total number of cells: {len(cells)}")

    _log.info("For each cell group the datasets by solar day")
    tasks = bin_by_solar_day(cells=cells)

    # Convert from dictionary to list of dictionaries.
    tasks = [{task_id: task_datasets} for task_id, task_datasets in tasks.items()]

    no_of_unique_datasets = len(
        set(
            chain.from_iterable(
                [task_datasets for task in tasks for task_id, task_datasets in task.items()]
            )
        )
    )
    _log.info(f"Total of {no_of_unique_datasets:,d} unique dataset UUIDs.")
    _log.info(f"Total number of tasks: {len(tasks)}")

    return tasks


def find_datasets_by_task_id(
    task_id: tuple[str, int, int],
    dc: Datacube,
    product: str,
) -> list[Dataset]:
    """
    Find all scenes overlapping with a tasks

    Parameters
    ----------
    task_id : tuple[str, int, int]
        Task id containing the solar day and tile x and y id
    dc : Datacube
    product : str
        Search for datasets belonging to the product.

    Returns
    -------
    list[Dataset]task
        Scenes covering the tile x and y id and matching the period in  the task id.
    """

    period, tile_id_x, tile_idy = task_id
    tile_id = (tile_id_x, tile_idy)

    tile_geobox = WaterbodiesGrid().gridspec.tile_geobox(tile_index=tile_id)

    scenes = dc.find_datasets(
        product=product, time=(period), like=tile_geobox, group_by="solar_day"
    )

    return scenes
