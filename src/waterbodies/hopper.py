import datetime
import logging
from types import SimpleNamespace
from warnings import warn

import toolz
from datacube.model import Dataset
from odc.dscache.tools import solar_offset
from odc.geo.geom import Geometry
from odc.stats.tasks import CompressedDataset, compress_ds
from odc.stats.utils import Cell
from tqdm import tqdm

from waterbodies.grid import WaterbodiesGrid

dt_range = SimpleNamespace(start=None, end=None)
_log = logging.getLogger(__name__)


# Copied from
# https://github.com/opendatacube/odc-dscache/blob/35c2f46e10f5b6cd64ae974b48f11ae2c34141d2/odc/dscache/tools/_index.py#L180C5-L180C23
# because of difference in behaviour between datacube.utils.geometry.Geometry
# and odc.geo.geom.Geometry
def bin_dataset_stream(gridspec, dss, cells, persist=None):
    """
    Intersect Grid Spec cells with Datasets.

    :param gridspec: GridSpec
    :param dss: Sequence of datasets (can be lazy)
    :param cells: Dictionary to populate with tiles
    :param persist: Dataset -> SomeThing mapping, defaults to keeping dataset id only

    The ``cells`` dictionary is a mapping from (x,y) tile index to object with the
    following properties:

     .idx     - tile index (x,y)
     .geobox  - tile geobox
     .utc_offset - timedelta to add to timestamp to get day component in local time
     .dss     - list of UUIDs, or results of ``persist(dataset)`` if custom ``persist`` is supplied
    """
    geobox_cache = {}

    def default_persist(ds):
        return ds.id

    def register(tile, geobox, val):
        cell = cells.get(tile)
        if cell is None:
            utc_ofset = solar_offset(geobox.extent)
            cells[tile] = SimpleNamespace(geobox=geobox, idx=tile, utc_offset=utc_ofset, dss=[val])
        else:
            cell.dss.append(val)

    if persist is None:
        persist = default_persist

    for ds in dss:
        ds_val = persist(ds)

        if ds.extent is None:
            warn(f"Dataset without extent info: {str(ds.id)}")
            continue

        for tile, geobox in gridspec.tiles_from_geopolygon(
            geopolygon=Geometry(geom=ds.extent.geom, crs=ds.extent.crs), geobox_cache=geobox_cache
        ):
            register(tile, geobox, ds_val)

        yield ds


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


def bin_by_tile_index(cells: dict[tuple[int, int], Cell]) -> dict[tuple[str, int, int], list[str]]:
    """
    Bin grid spec cells intersected with datasets by tile index.

    Parameters
    ----------
    cells : dict[tuple[int, int], Cell]
        Grid spec cells intersected with datasets to bin.

    Returns
    -------
    dict[tuple[str, int, int], list[str]]
        Tasks containing the task id (tile index x, tile index y) and Datasets UUIDs
        for the task.
    """
    tasks = {}
    for tile_index, cell in cells.items():
        tasks[tile_index] = cell.dss

    # Remove duplicate source uids
    # Duplicates occur when queried datasets are captured around UTC midnight
    # and around weekly boundary
    tasks = {task_id: [str(ds.id) for ds in set(dss)] for task_id, dss in tasks.items()}
    return tasks


def bin_by_solar_day(cells: dict[tuple[int, int], Cell]) -> dict[tuple[str, int, int], list[str]]:
    """
    Bin grid spec cells intersected with datasets by tile index and solar day.

    Parameters
    ----------
    cells : dict[tuple[int, int], Cell]
        Grid spec cells intersected with datasets to bin.

    Returns
    -------
    dict[tuple[str, int, int], list[str]]
        Tasks containing the task id (solar_day, tile index x, tile index y) and Datasets UUIDs
        for the task.
    """
    tasks = {}
    for tile_index, cell in cells.items():
        utc_offset = cell.utc_offset
        grouped_by_solar_day = toolz.groupby(lambda ds: (ds.time + utc_offset).date(), cell.dss)

        for solar_day_date, dss in grouped_by_solar_day.items():
            solar_day_key = (solar_day_date.strftime("%Y-%m-%d"),)
            tasks[solar_day_key + tile_index] = dss

    # Remove duplicate source uids
    # Duplicates occur when queried datasets are captured around UTC midnight
    # and around weekly boundary
    tasks = {task_id: [str(ds.id) for ds in set(dss)] for task_id, dss in tasks.items()}
    return tasks


def create_tasks_from_datasets(
    datasets: list[Dataset],
    tile_index_filter: list[tuple[int]] | None = None,
    bin_solar_day: bool = True,
) -> list[dict]:
    """
    Create a list of tasks to be processed from the datasets provided.

    Parameters
    ----------
    datasets : list[Dataset]
        A list of datasets to create tasks for.
    tile_index_filter : list[tuple[int]] | None, optional
        List of tile indices (x, y) for which tasks should be created. Each tuple
        represents the tile index in the format (tile_index_x, tile_index_y).
        If provided, only datasets matching the specified tile indices will be considered
        for task creation. If None, all datasets are considered.
    bin_solar_day : bool, optional
        If `bin_solar_day==True` tasks will be grouped by tile index and solar day. If
        `bin_solar_day==False`, tasks will only be grouped by tile index.
    Returns
    -------
    list[dict]
        A list of tasks, where each task is represented by a dictionary containing
        the task ID (solar_day, tile_index_x, tile_index_y) and UUIDs for the datasets
        matching the solar day and tile index (tile_index_x, tile_index_y) of the task.
    """

    cells = {}

    dss = bin_dataset_stream(
        gridspec=WaterbodiesGrid().gridspec, dss=datasets, cells=cells, persist=persist
    )

    with tqdm(
        iterable=dss, desc=f"Processing {len(datasets)} datasets", total=len(datasets)
    ) as dss:
        for _ in dss:
            pass

    if tile_index_filter:
        cells = {
            tile_index: cell
            for tile_index, cell in cells.items()
            if tile_index in tile_index_filter
        }
    else:
        pass

    if bin_solar_day:
        tasks = bin_by_solar_day(cells=cells)
    else:
        tasks = bin_by_tile_index(cells=cells)

    tasks = [{task_id: task_datasets_ids} for task_id, task_datasets_ids in tasks.items()]

    return tasks
