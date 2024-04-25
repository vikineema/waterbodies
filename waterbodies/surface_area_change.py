import datetime
import json
import logging

import numpy as np
import pandas as pd
import rioxarray
import xarray as xr
from datacube import Datacube
from skimage.measure import regionprops
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.io import find_geotiff_files
from waterbodies.text import get_task_id_str_from_tuple, get_tile_index_str_from_tuple

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
        1 = clear and wet
        0 = clear and dry
        2 = invalid (neither clear and wet or clear and dry)

    Parameters
    ----------wofl
    wofl : xr.Dataset
        WOfS Feature Layers dataset to mask

    Returns
    -------
    xr.DataArray
        Masked WOfS Feature Layers .water DataArray
    """
    INVALID_PIXEL_VALUE = 2
    keep_attrs = wofl.attrs

    # Use bitmasking values from DE Africa WOfS
    clear_and_wet = wofl.water == 128  # 1 if clear and wet, 0 otherwise
    clear_and_dry = wofl.water == 0  # 1 if clear and dry, 0 otherwise

    # create binary mask with 1 if clear wet or dry, 0 if not-clear
    clear_wet_or_dry = clear_and_wet | clear_and_dry

    # Start with clear and wet (1 if clear and wet, 0 otherwise)
    # Set all not-clear pixels to 2
    # Leaves clear and wet = 1, clear and dry = 0, all other values (invalid) = 2
    wofl_masked = clear_and_wet.where(clear_wet_or_dry, other=INVALID_PIXEL_VALUE)

    wofl_masked.attrs = keep_attrs

    return wofl_masked


def get_pixel_counts(region_mask, intensity_image):

    masked_intensity_image = intensity_image[region_mask]

    # Hard coded mask values from mask_wofl function
    DRY_PIXEL_VALUE = 0
    WET_PIXEL_VALUE = 1
    INVALID_PIXEL_VALUE = 2

    # Mask values can be any of 0, 1, or 2
    # Return values present in mask and their correpsonding pixel counts
    mask_values, mask_value_counts = np.unique(masked_intensity_image, return_counts=True)

    # Convert into dictionary of {mask_value: count}
    # E.g. {0: 100, 1: 50, 2:40}
    mask_values_and_counts = dict(zip(mask_values, mask_value_counts))

    # Get the number of pixels for each type, setting to 0 if not present in mask_values_and_counts
    px_invalid = mask_values_and_counts.get(INVALID_PIXEL_VALUE, 0)
    px_dry = mask_values_and_counts.get(DRY_PIXEL_VALUE, 0)
    px_wet = mask_values_and_counts.get(WET_PIXEL_VALUE, 0)
    px_total = px_invalid + px_dry + px_wet

    # Construct the counts as a dict for pandas dataframe
    pixel_counts = {
        "px_total": [px_total],
        "px_invalid": [px_invalid],
        "px_dry": [px_dry],
        "px_wet": [px_wet],
    }
    pixel_counts_df = pd.DataFrame(pixel_counts)
    return pixel_counts_df


def get_waterbody_observations(
    solar_day: str,
    tile_index_x: int,
    tile_index_y: int,
    task_datasets_ids: list[str],
    historical_extent_rasters_directory: str,
    dc: Datacube,
) -> pd.DataFrame | None:
    """
    Generate the waterbody observations for a given task

    Parameters
    ----------
    solar_day : str
        Solar day of the task.
    tile_index_x : int
        X tile index of the task.
    tile_index_y : int
        Y tile index of the task.
    task_datasets_ids: list[str]
        IDs of the datasets for the task.
    historical_extent_rasters_directory : str
        Directory containing the historical extent rasters.
    dc : Datacube
        Datacube connection

    Returns
    -------
    pd.DataFrame
        A DataFrame table containing the waterbody observations for the task

    """
    task_id = (solar_day, tile_index_x, tile_index_y)
    task_id_str = get_task_id_str_from_tuple(task_id)

    tile_index = (tile_index_x, tile_index_y)
    tile_index_str = get_tile_index_str_from_tuple(tile_index)

    historical_extent_raster_file = find_geotiff_files(
        directory_path=historical_extent_rasters_directory,
        file_name_pattern=tile_index_str,
    )
    if historical_extent_raster_file:
        historical_extent_raster = rioxarray.open_rasterio(
            historical_extent_raster_file[0]
        ).squeeze("band", drop=True)
        # Get the mapping of WB_ID to UID from the attributes.
        wbid_to_uid = json.loads(historical_extent_raster.attrs["WB_ID_to_UID"])
    else:
        e = FileNotFoundError(
            f"Tile {tile_index_str} does not have a historical extent "
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

    if polygons_pixel_counts:
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
                "uid",
                "date",
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
    else:
        return None


def add_waterbody_observations_to_db(
    waterbody_observations: pd.DataFrame,
    engine: Engine,
    update_rows: bool = True,
):
    """
    Add waterbody observations to the waterbodies observations table.

    Parameters
    ----------
    waterbody_observations : pd.DataFrame
        Table containing the waterbody observations to add to the database
    engine : Engine
    update_rows : bool, optional
        If True if the a waterbody observation id already exists in the table, the row
        will be updated else it will be skipped, by default True
    """

    # Ensure the waterbodies observation table exists.
    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(bind=engine)

    # Note: Doing it this way because drill outputs can be millions of rows.
    # Its best to do it in small batches.
    obs_ids_to_check = waterbody_observations["obs_id"].to_list()
    with Session.begin() as session:
        obs_ids_exist = session.scalars(
            select(table.c.obs_id).where(table.c.obs_id.in_(obs_ids_to_check))
        ).all()
        _log.info(
            f"Found {len(obs_ids_exist)} out of {len(obs_ids_to_check)} waterbody "
            f"observations already in the {table.name} table"
        )

    update_statements = []
    insert_parameters = []

    for row in waterbody_observations.itertuples():
        if row.obs_id not in obs_ids_exist:
            insert_parameters.append(
                dict(
                    obs_id=row.obs_id,
                    task_id=row.task_id,
                    date=row.date,
                    uid=row.uid,
                    px_total=row.px_total,
                    px_wet=row.px_wet,
                    area_wet_m2=row.area_wet_m2,
                    px_dry=row.px_dry,
                    area_dry_m2=row.area_dry_m2,
                    px_invalid=row.px_invalid,
                    area_invalid_m2=row.area_invalid_m2,
                )
            )
        else:
            if update_rows:
                update_statements.append(
                    update(table)
                    .where(table.c.obs_id == row.obs_id)
                    .values(
                        dict(
                            task_id=row.task_id,
                            date=row.date,
                            uid=row.uid,
                            px_total=row.px_total,
                            px_wet=row.px_wet,
                            area_wet_m2=row.area_wet_m2,
                            px_dry=row.px_dry,
                            area_dry_m2=row.area_dry_m2,
                            px_invalid=row.px_invalid,
                            area_invalid_m2=row.area_invalid_m2,
                        )
                    )
                )
            else:
                continue

    if update_statements:
        _log.info(
            f"Updating {len(update_statements)} waterbody observations in the {table.name} table"
        )
        with Session.begin() as session:
            for statement in update_statements:
                session.execute(statement)
    else:
        _log.info(f"No waterbody observations to update in the {table.name} table")

    if insert_parameters:
        _log.info(
            f"Inerting {len(insert_parameters)} waterbody observations in the {table.name} table"
        )
        with Session.begin() as session:
            session.execute(insert(table), insert_parameters)
    else:
        _log.error(f"No waterbody observations to insert into the {table.name} table")


def check_task_exists(
    task_id_str: str,
    engine: Engine,
) -> bool:
    """
    Check if a task already exists in the database by checking if
    any waterbody observation has a matching task id.

    Parameters
    ----------
    task_id_str : str
        Task id to check for.
    engine : Engine

    Returns
    -------
    bool
        True if a single waterbody observation with a matching task id has been found
        False if not.
    """
    Session = sessionmaker(bind=engine)

    table = create_waterbodies_observations_table(engine=engine)

    with Session.begin() as session:
        results = session.scalars(
            select(table.c.task_id).where(table.c.task_id == task_id_str)
        ).first()

    if results:
        return True
    else:
        return False
