import datetime
import json
import logging

import numpy as np
import pandas as pd
import rioxarray
import xarray as xr
from datacube import Datacube
from skimage.measure import regionprops
from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.io import find_geotiff_files
from waterbodies.text import task_id_tuple_to_str, tile_id_tuple_to_str

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


def get_pixel_counts(region_mask, intensity_image):

    masked_intensity_image = intensity_image[region_mask]

    dry_pixel_value = 0
    wet_pixel_value = 1
    invalid_pixel_value = -9999

    unique_values, unique_value_counts = np.unique(masked_intensity_image, return_counts=True)
    unique_values = np.where(np.isnan(unique_values), invalid_pixel_value, unique_values)
    unique_values_and_counts = dict(zip(unique_values, unique_value_counts))

    px_total = np.sum(unique_value_counts)
    px_invalid = unique_values_and_counts.get(invalid_pixel_value, np.nan)
    px_dry = unique_values_and_counts.get(dry_pixel_value, np.nan)
    px_wet = unique_values_and_counts.get(wet_pixel_value, np.nan)

    pixel_counts = {
        "px_total": [px_total],
        "px_invalid": [px_invalid],
        "px_dry": [px_dry],
        "px_wet": [px_wet],
    }
    pixel_counts_df = pd.DataFrame(pixel_counts)
    return pixel_counts_df


def get_waterbody_observations(
    task: dict[tuple[str, int, int], list[str]],
    historical_extent_rasters_directory: str,
    dc: Datacube,
) -> pd.DataFrame:

    assert len(task) == 1
    task_id, task_datasets_ids = next(iter(task.items()))

    solar_day, tile_id_x, tile_id_y = task_id

    tile_id_str = tile_id_tuple_to_str((tile_id_x, tile_id_y))
    task_id_str = task_id_tuple_to_str(task_id)

    historical_extent_raster_file = find_geotiff_files(
        directory_path=historical_extent_rasters_directory,
        file_name_pattern=tile_id_str,
    )
    if historical_extent_raster_file:
        historical_extent_raster = rioxarray.open_rasterio(
            historical_extent_raster_file[0]
        ).squeeze("band", drop=True)
        # Get the mapping of WB_ID to UID from the attributes.
        wbid_to_uid = json.loads(historical_extent_raster.attrs["WB_ID_to_UID"])
    else:
        e = FileNotFoundError(
            f"Tile {tile_id_str} does not have a historical extent "
            f"raster in the directory {historical_extent_rasters_directory}"
        )
        _log.error(e)
        raise e

    task_datasets = [dc.index.datasets.get(ds_id) for ds_id in task_datasets_ids]

    ds = dc.load(
        datasets=task_datasets, like=historical_extent_raster.geobox, group_by="solar_day"
    ).squeeze()

    da = mask_wofl(ds)

    region_properties = regionprops(
        label_image=historical_extent_raster.values,
        intensity_image=da.values,
        extra_properties=[get_pixel_counts],
    )

    polygons_pixel_counts = []
    for region_prop in region_properties:
        poly_pixel_counts_df = region_prop.get_pixel_counts
        poly_pixel_counts_df["uid"] = [wbid_to_uid[str(region_prop.label)]]
        polygons_pixel_counts.append(poly_pixel_counts_df)

    waterbody_observations = pd.concat(polygons_pixel_counts, ignore_index=True)

    px_area = abs(
        historical_extent_raster.geobox.resolution[0]
        * historical_extent_raster.geobox.resolution[1]
    )

    waterbody_observations["area_invalid_m2"] = waterbody_observations["px_invalid"] * px_area
    waterbody_observations["area_dry_m2"] = waterbody_observations["px_dry"] * px_area
    waterbody_observations["area_wet_m2"] = waterbody_observations["px_wet"] * px_area

    waterbody_observations["date"] = pd.to_datetime(solar_day)
    waterbody_observations["obs_id"] = waterbody_observations["uid"].apply(
        lambda x: f"{task_id_str}_{x}"
    )
    waterbody_observations["task_id"] = task_id_str

    # Reorder how the columns appear
    waterbody_observations = waterbody_observations[
        [
            "obs_id",
            "task_id",
            "date",
            "uid",
            "px_total",
            "px_wet",
            "area_wet_m2",
            "px_dry",
            "area_dry_m2",
            "px_invalid",
            "area_invalid_m2",
        ]
    ]
    return waterbody_observations




def add_waterbody_observations_to_db(
    waterbody_observations: pd.DataFrame,
    engine: Engine,
):
    # Ensure the waterbodies observation table exists.
    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(bind=engine)

    # Get the observation ids.abs
    obs_ids_to_check = waterbody_observations["obs_id"].to_list()

    with Session.begin() as session: