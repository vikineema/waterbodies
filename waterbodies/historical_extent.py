import logging

import geopandas as gpd
import numpy as np
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

    assert all([col in waterbodies_polygons.columns for col in ["UID", "WB_ID"]])

    assert waterbodies_polygons["UID"].is_unique

    assert waterbodies_polygons["WB_ID"].is_unique
    assert waterbodies_polygons["WB_ID"].min() > 0

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
        if row.UID not in uids:
            insert_parameters.append(
                dict(
                    uid=row.UID,
                    area_m2=row.area_m2,
                    wb_id=row.WB_ID,
                    length_m=row.length_m,
                    perim_m=row.perim_m,
                    geometry=f"SRID={srid};{row.geometry.wkt}",
                )
            )
        else:
            if update_rows:
                update_statements.append(
                    update(table)
                    .where(table.c.uid == row.UID)
                    .values(
                        dict(
                            area_m2=row.area_m2,
                            wb_id=row.WB_ID,
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


def get_waterbodies(
    tile_index_x: int,
    tile_index_y: int,
    task_datasets_ids: list[str],
    dc: Datacube,
    goas_rasters_directory: str,
    location_threshold: float = 0.1,
    extent_threshold: float = 0.05,
    min_valid_observations: int = 60,
) -> gpd.GeoDataFrame:
    """
    Generate the waterbody polygons for a given task.

    Parameters
    ----------
    tile_index_x : int
        X tile index of the task.
    tile_index_y : int
        Y tile index of the task.
    task_datasets_ids : list[str]
        IDs of the datasets for the task.
    dc : Datacube
        Datacube connection
    goas_rasters_directory : str
        Directory containing the Global Oceans and Seas version 1 rasters.
    location_threshold : float, optional
        Threshold used to set the location of the waterbody polygons, by default 0.1
    extent_threshold : float, optional
        Threshold used to set the shape/extent of the waterbody polygons, by default 0.05
    min_valid_observations : int, optional
        Threshold to use to mask out pixels based on the number of valid WOfS
        observations for each pixel, by default 60

    Returns
    -------
    gpd.GeoDataFrame
        Waterbody polygons for the task.
    """
    tile_index = (tile_index_x, tile_index_y)
    tile_index_str = get_tile_index_str_from_tuple(tile_index)
    gridspec = WaterbodiesGrid().gridspec
    tile_geobox = gridspec.tile_geobox(tile_index=tile_index)

    task_datasets = [dc.index.datasets.get(ds_id) for ds_id in task_datasets_ids]

    goas_raster_file = find_geotiff_files(
        directory_path=goas_rasters_directory, file_name_pattern=tile_index_str
    )
    if goas_raster_file:
        # Load the global oceans and seas raster for the tile.
        # Convert the oceans/seas pixels from 1 to 0 and the land pixels from 0 to 1.
        land_sea_mask = np.logical_not(
            rio_slurp_xarray(fname=goas_raster_file[0], gbox=tile_geobox)
        ).astype(int)
        # Erode the land pixels by 500 m
        eroded_land_sea_mask = binary_erosion(
            image=land_sea_mask.values,
            footprint=disk(radius=500 / abs(land_sea_mask.geobox.resolution[0])),
        )
    else:
        e = FileNotFoundError(
            f"Tile {tile_index_str} does not have a Global Oceans and Seas "
            f"raster in the directory {goas_rasters_directory}"
        )
        _log.error(e)
        raise e

    # Note: It is expected that for the wofs_ls_summary_alltime product
    # there is one time step for each tile, however in case of
    # multiple, pick the most recent time.
    ds = (
        dc.load(datasets=task_datasets, measurements=["count_clear", "frequency"], like=tile_geobox)
        .isel(time=-1)
        .where(eroded_land_sea_mask)
    )

    ds["count_clear"] = ds["count_clear"].where(ds["count_clear"] != -999)
    valid_clear_count = ds["count_clear"] >= min_valid_observations

    valid_location = np.logical_and(ds["frequency"] > location_threshold, valid_clear_count).astype(
        int
    )
    valid_extent = np.logical_and(ds["frequency"] > extent_threshold, valid_clear_count).astype(int)

    # Label connected regions in the valid_extent DataArray
    # each region is considered a waterbody.
    labelled_waterbodies = label(label_image=valid_extent.values, background=0)
    # Remove waterbodies smaller than 5 pixels.
    labelled_waterbodies = remove_small_objects(labelled_waterbodies, min_size=5, connectivity=1)

    # Identify waterbodies larger than 1000 pixels.
    large_waterbodies_labels = [
        region.label
        for region in regionprops(label_image=labelled_waterbodies)
        if region.num_pixels > 1000
    ]
    # Create a binary mask of the large waterbodies
    large_waterbodies_mask = np.where(np.isin(labelled_waterbodies, large_waterbodies_labels), 1, 0)
    # Remove the large waterbodies from the labelled image.
    labelled_waterbodies = np.where(large_waterbodies_mask == 1, 0, labelled_waterbodies)
    # Erode the location threshold pixels by 1.
    valid_location_eroded = erosion(image=valid_location.values, footprint=disk(radius=1))
    # Create the watershed segmentation markers by labelling the eroded image.
    watershed_segmentation_markers = label(label_image=valid_location_eroded, background=0)
    # Remove markers smaller than 100 pixels.
    watershed_segmentation_markers = remove_small_objects(
        watershed_segmentation_markers, min_size=100, connectivity=1
    )
    # Segment the large waterbodies using watershed segmentation
    segmented_large_waterbodies = watershed(
        image=-distance_transform_edt(large_waterbodies_mask),
        markers=watershed_segmentation_markers,
        mask=large_waterbodies_mask,
    )
    # Add the segmented large waterbodies.
    labelled_waterbodies = np.where(
        large_waterbodies_mask == 1, segmented_large_waterbodies, labelled_waterbodies
    )

    # Only keep waterbodies that contain a waterbody pixel from the valid_location DataArray.
    def waterbody_pixel_count(regionmask, intensity_image):
        return np.sum(intensity_image[regionmask])

    valid_waterbodies_labels = [
        region.label
        for region in regionprops(
            label_image=labelled_waterbodies,
            intensity_image=valid_location.values,
            extra_properties=[waterbody_pixel_count],
        )
        if region.waterbody_pixel_count > 0
    ]
    valid_waterbodies = np.where(
        np.isin(labelled_waterbodies, valid_waterbodies_labels), labelled_waterbodies, 0
    )
    # Relabel the waterbodies and remove waterbodies smaller than 6 pixels.
    valid_waterbodies = label(label_image=valid_waterbodies, background=0)
    valid_waterbodies = remove_small_objects(valid_waterbodies, min_size=6, connectivity=1)

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
