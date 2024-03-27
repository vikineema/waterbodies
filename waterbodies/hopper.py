import datetime
import logging
from types import SimpleNamespace

import toolz
from datacube import Datacube
from datacube.model import Dataset
from odc.dscache.tools import bin_dataset_stream
from odc.dscache.tools.tiling import parse_gridspec_with_name
from odc.stats.tasks import CompressedDataset, compress_ds
from odc.stats.utils import Cell
from tqdm import tqdm

dt_range = SimpleNamespace(start=None, end=None)

_log = logging.getLogger(__name__)


def find_datasets_by_creation_date(
    product: str,
    start_date: datetime.date,
    end_date: datetime.date,
    dc: Datacube = None,
) -> list[Dataset]:
    """
    Search for datasets in the datacube using the creation time metadata

    Parameters
    ----------
    product : str
        Search for datasets belonging to the product.
    start_date : datetime.date
        Start date for creation time range.
    end_date : datetime.date
        End date for creation time range.
    dc : Datacube, optional

    Returns
    -------
    list[Dataset]
        Datasets found matching the product and creation time range specified.
    """

    if dc is None:
        dc = Datacube()

    assert end_date >= start_date

    creation_time_range = (start_date, end_date)

    datasets = dc.find_datasets(product=[product], creation_time=creation_time_range)

    return datasets


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


def create_tasks(
    scenes: list[Dataset], tile_ids_of_interest: list[tuple[int]] = []
) -> dict[tuple[str, int, int], list[str]]:
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
    dict[tuple[str, int, int], list[str]]
        Tasks containing the task id (solar_day, tile id x, tile id y) and Datasets UUIDs
        for the task.
    """

    cells = {}
    grid_name = "africa_30"

    grid, gridspec = parse_gridspec_with_name(grid_name)
    dss = bin_dataset_stream(gridspec, scenes, cells, persist=persist)

    with tqdm(iterable=dss, desc=f"Processing {len(scenes):8,d} scenes", total=len(scenes)) as dss:
        for _ in dss:
            pass

    if tile_ids_of_interest:
        _log.info(
            f"Filter the {len(cells)} cells to keep only the cells"
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

    _log.info(f"Total of {len(set(list(tasks.values))):,d} unique dataset UUIDs.")
    _log.info(f"Total number of tasks: {len(tasks)}")

    return tasks
