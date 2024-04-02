import datetime
import logging

import rioxarray
import xarray as xr
from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.io import find_geotiff_files

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

    Parameters
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


def mask_wofl(wofl: xr.Dataset) -> xr.DataArray:
    """
    Apply WOfS bitmasking to a WOfS Feature Layers dataset
    to obtain a masked DataArray with the values:
        1 = water
        0 = dry
        np.nan = invalid (not wet or dry)

    Parameters
    ----------wofl
    wofl : xr.Dataset
        WOfS Feature Layers dataset to mask

    Returns
    -------
    xr.DataArray
        Masked WOfS Feature Layers .water DataArray
    """
    keep_attrs = wofl.attrs

    clear_and_wet = wofl.water == 128
    clear_and_dry = wofl.water == 0

    clear = clear_and_wet | clear_and_dry

    # Set the invalid (not clear) pixels to np.nan
    # Remaining values will be 1 if water, 0 if dry
    wofl_masked = clear_and_wet.where(clear)

    wofl_masked.attrs = keep_attrs

    return wofl_masked


def get_waterbody_observations(
    task: dict[tuple[str, int, int], list[str]],
    historical_extent_rasters_directory: str,
):

    task_id, task_datasets_ids = next(iter(task.items()))

    solar_day, tile_id_x, tile_id_y = task_id

    historical_extent_raster = find_geotiff_files(
        directory_path=historical_extent_rasters_directory,
        file_name_pattern=f"x{tile_id_x:03}_y{tile_id_y:03}",
    )
    if not historical_extent_raster:
        e = FileNotFoundError(
            f"Tile {(tile_id_x, tile_id_y)} does not have a historical extent "
            f"raster in the directory {historical_extent_rasters_directory}"
        )
        _log.error(e)
        raise e
    else:
        historical_extent_raster_da = rioxarray.open_rasterio(historical_extent_raster[0]).squeeze(
            "band", drop=True
        )
