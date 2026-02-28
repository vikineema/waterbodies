import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from datacube import Datacube
from rasterio.features import shapes
from scipy.ndimage import distance_transform_edt
from shapely.geometry import Point, Polygon, shape
from skimage.measure import regionprops
from skimage.morphology import (
    binary_erosion,
    disk,
    erosion,
    label,
    remove_small_objects,
)
from skimage.segmentation import watershed
from sqlalchemy import insert, select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyHistoricalExtent
from waterbodies.grid import WaterbodiesGrid
from waterbodies.io import find_geotiff_files
from waterbodies.text import get_tile_index_str_from_tuple
from waterbodies.utils import rio_slurp_xarray

_log = logging.getLogger(__name__)


def validate_waterbodies_polygons(waterbodies_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Validate the waterbodies_polygons GeoDataFrame to ensure it's
    structure is as expected.

    Parameters
    ----------
    waterbodies_polygons : gpd.GeoDataFrame
        The waterbodies polygons

    Returns
    -------
    gpd.GeoDataFrame
        The waterbodies polygons if all assertions passed.
    """

    assert all([col in waterbodies_polygons.columns for col in ["uid", "wb_id"]])

    assert waterbodies_polygons["uid"].is_unique

    assert waterbodies_polygons["wb_id"].is_unique
    assert waterbodies_polygons["wb_id"].min() > 0

    return waterbodies_polygons


def create_waterbodies_historical_extent_table(engine: Engine) -> Table:
    """
    Create the waterbodies_historical_extent_table if
    it does not exist.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    Table
        waterbodies_historical_extent table
    """
    table = create_table(engine=engine, db_model=WaterbodyHistoricalExtent)
    return table


def add_waterbodies_polygons_to_db(
    waterbodies_polygons: gpd.GeoDataFrame,
    engine: Engine,
    update_rows: bool = True,
):
    """
    Add the waterbodies polygons to the waterbodies
    historical exent table.

    Parameters
    ----------
    waterbodies_polygons : gpd.GeoDataFrame
        The waterbodies to be added to the database.
    engine : Engine
    update_rows : bool, optional
         If True if the polygon uid already exists in the waterbodies table, the row will be
         updated else it will be skipped, by default True

    """
    waterbodies_polygons = validate_waterbodies_polygons(waterbodies_polygons)

    # Ensure historical extent table exists
    table = create_waterbodies_historical_extent_table(engine=engine)

    Session = sessionmaker(bind=engine)

    update_statements = []
    insert_parameters = []

    with Session.begin() as session:
        uids = session.scalars(select(table.c["uid"])).all()
        _log.info(f"Found {len(uids)} polygon UIDs in the {table.name} table")

    srid = waterbodies_polygons.crs.to_epsg()

    for row in waterbodies_polygons.itertuples():
        if row.uid not in uids:
            insert_parameters.append(
                dict(
                    uid=row.uid,
                    area_m2=row.area_m2,
                    wb_id=row.wb_id,
                    length_m=row.length_m,
                    perim_m=row.perim_m,
                    geometry=f"SRID={srid};{row.geometry.wkt}",
                )
            )
        else:
            if update_rows:
                update_statements.append(
                    update(table)
                    .where(table.c.uid == row.uid)
                    .values(
                        dict(
                            area_m2=row.area_m2,
                            wb_id=row.wb_id,
                            length_m=row.length_m,
                            perim_m=row.perim_m,
                            geometry=f"SRID={srid};{row.geometry.wkt}",
                        )
                    )
                )
            else:
                continue

    if update_statements:
        _log.info(f"Updating {len(update_statements)} polygons in the {table.name} table")
        with Session.begin() as session:
            for statement in update_statements:
                session.execute(statement)
    else:
        _log.info(f"No polygons to update in the {table.name} table")

    if insert_parameters:
        _log.info(f"Adding {len(insert_parameters)} polygons to the {table.name} table")
        with Session.begin() as session:
            session.execute(insert(table), insert_parameters)
    else:
        _log.error(f"No polygons to insert into the {table.name} table")


def load_waterbodies_from_db(engine: Engine) -> gpd.GeoDataFrame:
    """
    Load all waterbodies polygons from the `waterbodies_historical_extent`
    table.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    gpd.GeoDataFrame
        All waterbodies polygons present in the `waterbodies_historical_extent` table.
    """

    table = create_waterbodies_historical_extent_table(engine=engine)
    table_name = table.name

    sql_query = f"SELECT * FROM {table_name}"

    waterbodies = gpd.read_postgis(sql_query, engine, geom_col="geometry")

    return waterbodies


def load_wofs_frequency(
    tile_index_x: int,
    tile_index_y: int,
    task_datasets_ids: list[str],
    dc: Datacube,
    land_sea_mask_rasters_directory: str,
    detection_threshold: float = 0.1,
    extent_threshold: float = 0.05,
    min_valid_observations: int = 60,
) -> tuple[xr.DataArray, xr.DataArray]:
    """
    Load the WOfS All-Time Summary frequency measurement for a tile and threshold the data
    using the extent and the detection thresholds.

    Parameters
    ----------
    tile_index_x : int
        X value of the tile index of the tile to generate waterbody polygons for.
    tile_index_y : int
        Y value of the tile index of the tile to generate waterbody polygons for.
    task_datasets_ids : list[str]
        UUID(s) of the WOfS All Time Summary dataset(s) that cover the tile to generate
        waterbody polygons for.
    dc : Datacube
        Datacube connection
    land_sea_mask_rasters_directory : str
        Directory containing the rasters to use to mask ocean and sea pixels.
    detection_threshold : float, optional
        Threshold to use to set the location of the waterbody polygons, by default 0.1
    extent_threshold : float, optional
        Threshold to use to set the shape/extent of the waterbody polygons, by default 0.05
    min_valid_observations : int, optional
        Threshold to use to mask out pixels based on the number of valid WOfS
        observations for each pixel, by default 60

    Returns
    -------
    tuple[xr.DataArray, xr.DataArray]
        WOfS All Time Summary frequency measurement thresholded using the detection threshold
        WOfS All Time Summary frequency measurement thresholded using the extent threshold
    """

    tile_index = (tile_index_x, tile_index_y)
    tile_index_str = get_tile_index_str_from_tuple(tile_index)
    task_datasets = [dc.index.datasets.get(ds_id) for ds_id in task_datasets_ids]

    measurements = ["count_clear", "frequency"]
    dc_query = dict(datasets=task_datasets, measurements=measurements)

    if not land_sea_mask_rasters_directory:
        _log.info(
            f"Skip masking ocean and sea pixels for tile {tile_index_str}, "
            "no land/sea mask rasters directory provided."
        )
        gridspec = WaterbodiesGrid().gridspec
        tile_geobox = gridspec.tile_geobox(tile_index=tile_index)
        # Note: It is expected that for the wofs_ls_summary_alltime product
        # there is one time step for each tile, however in case of
        # multiple, pick the earliest time.
        ds = dc.load(like=tile_geobox, **dc_query).isel(time=0)
    else:
        land_sea_mask_raster_file = find_geotiff_files(
            directory_path=land_sea_mask_rasters_directory, file_name_pattern=tile_index_str
        )
        if land_sea_mask_raster_file:
            # Load the land/sea mask raster for the tile.
            # Note: in the land/sea mask raster oceans/seas pixels must have a value of 0
            # and the land pixels a value of 1 and the same extent/geobox as the tile.
            land_sea_mask = rio_slurp_xarray(fname=land_sea_mask_raster_file[0])
            # Erode the land pixels by 500 m
            eroded_land_sea_mask = binary_erosion(
                image=land_sea_mask.values,
                footprint=disk(radius=500 / abs(land_sea_mask.geobox.resolution[0])),
            )
            # Mask the WOfS data using the land sea mask
            ds = dc.load(like=land_sea_mask.odc.geobox, **dc_query).isel(time=0)
            ds = ds.where(eroded_land_sea_mask)
        else:
            _log.info(
                f"Tile {tile_index_str} does not have a land/sea mask "
                f"raster in the directory {land_sea_mask_rasters_directory}"
            )
            gridspec = WaterbodiesGrid().gridspec
            tile_geobox = gridspec.tile_geobox(tile_index=tile_index)
            # Note: It is expected that for the wofs_ls_summary_alltime product
            # there is one time step for each tile, however in case of
            # multiple, pick the earliest time.
            ds = dc.load(like=tile_geobox, **dc_query).isel(time=0)

    # Threshold using the extent and detection thresholds.
    ds["count_clear"] = ds["count_clear"].where(ds["count_clear"] != -999)
    valid_clear_count = ds["count_clear"] >= min_valid_observations
    valid_detection = np.logical_and(
        ds["frequency"] > detection_threshold, valid_clear_count
    ).astype(int)
    valid_extent = np.logical_and(ds["frequency"] > extent_threshold, valid_clear_count).astype(int)
    return valid_detection, valid_extent


def remove_small_waterbodies(waterbodies_raster: np.ndarray, min_size: int) -> np.ndarray:
    """
    Label connected regions in the `waterbodies_raster` array and remove waterbodies (regions)
    smaller than the specified number of pixels.
    Parameters
    ----------
    waterbodies_raster : np.ndarray
        Raster image to filter.
    min_size : int
        The smallest allowable waterbody size i.e. minimum number of pixels a waterbody must have.

    Returns
    -------
    np.ndarray
        Labelled raster image with the waterbodies (regoins) smaller than specified number
        of pixels removed.
    """
    labelled_waterbodies_raster = label(label_image=waterbodies_raster, background=0)
    labelled_waterbodies_raster = remove_small_objects(
        labelled_waterbodies_raster, min_size=min_size, connectivity=1
    )
    return labelled_waterbodies_raster


def select_large_waterbodies(labelled_waterbodies_raster: np.ndarray, min_size: int) -> np.ndarray:
    """
    Identify waterbodies (regions) larger than the specified number of pixels and create a binary
    mask of the large waterbodies.

    Parameters
    ----------
    labelled_waterbodies_raster : np.ndarray
        Labelled raster image to filter.
    min_size : int
        Minimum number of pixels to classify a waterbody as large.

    Returns
    -------
    np.ndarray
        Binary mask of the large waterbodies.
    """
    large_waterbodies_labels = [
        region.label
        for region in regionprops(label_image=labelled_waterbodies_raster)
        if region.num_pixels > min_size
    ]

    large_waterbodies_mask = np.where(
        np.isin(labelled_waterbodies_raster, large_waterbodies_labels), 1, 0
    )

    return large_waterbodies_mask


def generate_watershed_segmentation_markers(
    marker_source: np.ndarray, erosion_radius: int, min_size: int
) -> np.ndarray:
    """
    Create watershed segmentation markers by eroding the marker source pixels
    and labelling the resulting image.

    Parameters
    ----------
    marker_source : np.ndarray
        Raster image to generate watershed segmentation markers from.
    erosion_radius : int
        Radius to use to generate footprint for erosion.
    min_size : int
        The smallest allowable marker size.

    Returns
    -------
    np.ndarray
        Watershed segmentation markers.
    """
    eroded_marker_source = erosion(image=marker_source, footprint=disk(radius=erosion_radius))
    watershed_segmentation_markers = label(label_image=eroded_marker_source, background=0)
    watershed_segmentation_markers = remove_small_objects(
        watershed_segmentation_markers, min_size=min_size, connectivity=1
    )
    return watershed_segmentation_markers


def segment_waterbodies(
    waterbodies_to_segment: np.ndarray, segmentation_markers: np.ndarray
) -> np.ndarray:
    """
    Segment waterbodies.

    Parameters
    ----------
    waterbodies_to_segment : np.ndarray
        Raster image containing the waterbodies to be segmented.
    segmentation_markers : np.ndarray
        Raster image containing the watershed segmentation markers.

    Returns
    -------
    np.ndarray
        Raster image with the waterbodies segmented.
    """
    segmented_waterbodies = watershed(
        image=-distance_transform_edt(waterbodies_to_segment),
        markers=segmentation_markers,
        mask=waterbodies_to_segment,
    )
    return segmented_waterbodies


def confirm_extent_contains_detection(
    extent_waterbodies: np.ndarray, detection_np: np.ndarray
) -> np.ndarray:
    """
    Filter the waterbodies in the extent raster to keep only waterbodies that contain a
    waterbody pixel from the detection raster.

    Parameters
    ----------
    extent_waterbodies : np.ndarray
        Raster of the extent of the waterbodies.
    detection_np : np.ndarray
        Raster of the location of the waterbodies.

    Returns
    -------
    np.ndarray
        Filtered waterbodies in the extent raster.
    """

    def detection_pixel_count(regionmask, intensity_image):
        return np.sum(intensity_image[regionmask])

    valid_waterbodies_labels = [
        region.label
        for region in regionprops(
            label_image=extent_waterbodies,
            intensity_image=detection_np,
            extra_properties=[detection_pixel_count],
        )
        if region.detection_pixel_count > 0
    ]
    valid_waterbodies = np.where(
        np.isin(extent_waterbodies, valid_waterbodies_labels), extent_waterbodies, 0
    )
    return valid_waterbodies


def get_waterbodies(
    tile_index_x: int,
    tile_index_y: int,
    task_datasets_ids: list[str],
    dc: Datacube,
    land_sea_mask_rasters_directory: str,
    detection_threshold: float = 0.1,
    extent_threshold: float = 0.05,
    min_valid_observations: int = 60,
    min_polygon_size: int = 6,
    max_polygon_size: int = 1000,
) -> gpd.GeoDataFrame:
    """
    Generate the waterbody polygons for a given tile.

    Parameters
    ----------
    tile_index_x : int
        X value of the tile index of the tile to generate waterbody polygons for.
    tile_index_y : int
        Y value of the tile index of the tile to generate waterbody polygons for.
    task_datasets_ids : list[str]
        UUID(s) of the WOfS All Time Summary dataset(s) that cover the tile to generate
        waterbody polygons for.
    dc : Datacube
        Datacube connection
    land_sea_mask_rasters_directory : str
        Directory containing the rasters to use for masking ocean and sea pixels.
    detection_threshold : float, optional
        Threshold to use to set the location of the waterbody polygons, by default 0.1
    extent_threshold : float, optional
        Threshold to use to set the shape/extent of the waterbody polygons, by default 0.05
    min_valid_observations : int, optional
        Threshold to use to mask out pixels based on the number of valid WOfS
        observations for each pixel, by default 60
    min_polygon_size : int, optional
        Minimum number of pixels a waterbody must have to be included, by default 6
    max_polygon_size : int, optional
        Maximum number of pixels a waterbody can have. Waterbodies larger than the specified number
        of pixels are segmentated using watershed segmentation, by default 1000

    Returns
    -------
    gpd.GeoDataFrame
        Waterbody polygons for a given tile
    """
    detection_da, extent_da = load_wofs_frequency(
        tile_index_x=tile_index_x,
        tile_index_y=tile_index_y,
        task_datasets_ids=task_datasets_ids,
        dc=dc,
        land_sea_mask_rasters_directory=land_sea_mask_rasters_directory,
        detection_threshold=detection_threshold,
        extent_threshold=extent_threshold,
        min_valid_observations=min_valid_observations,
    )

    extent_waterbodies = remove_small_waterbodies(
        waterbodies_raster=extent_da.values, min_size=min_polygon_size
    )

    extent_large_waterbodies_mask = select_large_waterbodies(
        labelled_waterbodies_raster=extent_waterbodies, min_size=max_polygon_size
    )

    # Remove the large waterbodies from the labelled image.
    extent_waterbodies = np.where(extent_large_waterbodies_mask == 1, 0, extent_waterbodies)

    watershed_segmentation_markers = generate_watershed_segmentation_markers(
        marker_source=detection_da.values, erosion_radius=1, min_size=100
    )

    # Segment the large waterbodies using watershed segmentation
    segmented_extent_large_waterbodies = segment_waterbodies(
        waterbodies_to_segment=extent_large_waterbodies_mask,
        segmentation_markers=watershed_segmentation_markers,
    )

    # Add the segmented large waterbodies.
    extent_waterbodies = np.where(
        segmented_extent_large_waterbodies > 0,
        segmented_extent_large_waterbodies,
        extent_waterbodies,
    )

    valid_waterbodies = confirm_extent_contains_detection(
        extent_waterbodies=extent_waterbodies, detection_np=detection_da.values
    )

    # Relabel the waterbodies and remove waterbodies smaller than 6 pixels.
    valid_waterbodies = remove_small_waterbodies(
        waterbodies_raster=valid_waterbodies, min_size=min_polygon_size
    )

    tile_index = (tile_index_x, tile_index_y)
    gridspec = WaterbodiesGrid().gridspec
    tile_geobox = gridspec.tile_geobox(tile_index=tile_index)

    # Vectorize the waterbodies.
    polygon_value_pairs = list(
        shapes(
            source=valid_waterbodies.astype(np.int32),
            mask=valid_waterbodies > 0,
            transform=tile_geobox.transform,
        )
    )
    polygons = [shape(polygon_value_pair[0]) for polygon_value_pair in polygon_value_pairs]
    polygons_gdf = gpd.GeoDataFrame(geometry=polygons, crs=tile_geobox.crs)
    return polygons_gdf


def get_polygon_length(poly: Polygon) -> float:
    """
    Calculate the length of a polygon.

    Parameters
    ----------
    poly : Polygon
        Polygon to get length for.

    Returns
    -------
    float
        Length of polygon i.e. longest edge of the mminimum bounding of the polygon.
    """
    # Calculate the minimum bounding box (oriented rectangle) of the polygon
    min_bbox = poly.minimum_rotated_rectangle

    # Get the coordinates of polygon vertices.
    x, y = min_bbox.exterior.coords.xy

    # Get the length of bounding box edges
    edge_length = (
        Point(x[0], y[0]).distance(Point(x[1], y[1])),
        Point(x[1], y[1]).distance(Point(x[2], y[2])),
    )

    # Get the length of polygon as the longest edge of the bounding box.
    length = max(edge_length)

    # Get width of the polygon as the shortest edge of the bounding box.
    # width = min(edge_length)

    return length


def get_waterbody_timeseries(
    engine: Engine, uid: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """
    Get the timeseries for a waterbody.
    Parameters
    ----------
    engine : Engine
    uid : str
        Unique string ID for a waterbody
    start_date: str
        Start date for the time range to filter the timeseries to.
    end_date: str
        End date for the time range to filter the timeseries to.

    Returns
    -------
    pd.DataFrame
        Timeseries for the waterbody.
    """
    # Get the actual area of the waterbody
    part_1 = (
        "WITH wb AS "
        "(SELECT uid, area_m2 AS actual_area_m2 FROM waterbodies_historical_extent "
        f"WHERE uid = '{uid}')"
    )

    # Get all observations for the waterbody.
    part_2 = (
        "wbo AS "
        "(SELECT wo.*, wb.actual_area_m2 FROM waterbodies_observations AS wo "
        "INNER JOIN wb ON wo.uid = wb.uid "
        f"WHERE wo.date BETWEEN '{start_date}' AND '{end_date}')"
    )

    # Get timeseries statistics from observations
    part_3 = (
        "waterbody_stats AS "
        "(SELECT date,  SUM(area_wet_m2) AS area_wet_m2, SUM(area_dry_m2) AS area_dry_m2, "
        "SUM(area_invalid_m2) AS area_invalid_m2, "
        "SUM(area_wet_m2 + area_dry_m2 + area_invalid_m2) AS area_observed_m2, "
        "actual_area_m2 FROM  wbo GROUP BY date, actual_area_m2)"
    )
    part_4 = (
        "waterbody_stats_pc AS "
        "(SELECT date, area_wet_m2, (area_wet_m2/actual_area_m2) * 100 AS percent_wet, "
        "area_dry_m2, (area_dry_m2/actual_area_m2) * 100 AS percent_dry, area_invalid_m2, "
        "(area_invalid_m2/actual_area_m2) * 100 AS percent_invalid, area_observed_m2, "
        "(area_observed_m2/actual_area_m2) * 100 AS percent_observed FROM waterbody_stats)"
    )

    final_select_query = "SELECT * FROM waterbody_stats_pc ORDER BY date"
    sql_query = f"{part_1}, {part_2}, {part_3}, {part_4} {final_select_query}"

    timeseries = pd.read_sql(con=engine, sql=sql_query)

    return timeseries
