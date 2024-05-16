import logging

import click
import geohash as gh
import geopandas as gpd
import pandas as pd
from datacube import Datacube
from shapely.ops import unary_union

from waterbodies.db import get_waterbodies_engine
from waterbodies.grid import WaterbodiesGrid
from waterbodies.historical_extent import (
    add_waterbodies_polygons_to_db,
    get_polygon_length,
)
from waterbodies.hopper import create_tasks_from_datasets
from waterbodies.io import find_parquet_files
from waterbodies.logs import logging_setup


@click.command(
    name="process-polygons",
    help="Process the waterbodies from the process-tasks step into the historical extent dataset.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--polygons-directory",
    type=str,
    help="Directory containing the waterbodies files from the process-tasks step.",
)
def process_polygons(verbose, polygons_directory):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dc = Datacube(app="process-polygons")

    files = find_parquet_files(polygons_directory)
    _log.info(f"Found {len(files)} files containing waterbodies.")

    gridspec = WaterbodiesGrid().gridspec

    waterbodies_list = []
    for file in files:
        gdf = gpd.read_file(file).to_crs(gridspec.crs)
        waterbodies_list.append(gdf)

    waterbodies = pd.concat(waterbodies_list, ignore_index=True)
    _log.info(f"Loaded {len(waterbodies)} waterbodies.")

    # Get all the tiles used to generate the waterbodies.
    datasets = dc.find_datasets(product="wofs_ls_summary_alltime")
    tasks = create_tasks_from_datasets(
        datasets=datasets, tile_index_filter=None, bin_solar_day=False
    )
    tile_indices = [k for task in tasks for k, v in task.items()]
    buffered_tile_boundaries = [
        gridspec.tile_geobox(tile_index=tile_index).extent.geom.boundary.buffer(
            30, cap_style="flat", join_style="mitre"
        )
        for tile_index in tile_indices
    ]
    buffered_tile_boundaries_gdf = gpd.GeoDataFrame(
        data={"tile_index": tile_indices, "geometry": buffered_tile_boundaries}, crs=gridspec.crs
    )
    buffered_tile_boundaries_gdf.set_index("tile_index", inplace=True)
    _log.info(f"Found {len(buffered_tile_boundaries_gdf)} tiles")

    _log.info("Merging waterbodies at tile boundaries...")
    joined = gpd.sjoin(
        waterbodies, buffered_tile_boundaries_gdf, how="inner", predicate="intersects"
    )
    if joined.empty:
        pass
    else:
        tile_boundary_waterbodies = waterbodies[waterbodies.index.isin(joined.index)]
        not_tile_boundary_waterbodies = waterbodies[~waterbodies.index.isin(joined.index)]
        tile_boundary_waterbodies_merged = (
            gpd.GeoDataFrame(
                crs=gridspec.crs, geometry=[unary_union(tile_boundary_waterbodies.geometry)]
            )
            .explode(index_parts=True)
            .reset_index(drop=True)
        )
        waterbodies = pd.concat(
            [not_tile_boundary_waterbodies, tile_boundary_waterbodies_merged],
            ignore_index=True,
            sort=True,
        )
    _log.info(f"Waterbodies count after merging waterbodies at tile boundaries: {len(waterbodies)}")

    waterbodies["area_m2"] = waterbodies.geometry.area
    waterbodies = waterbodies[waterbodies.area_m2 > 4500]
    _log.info(
        f"Waterbodies count after filtering out waterbodies smaller than 4500m2: {len(waterbodies)}"
    )

    waterbodies["length_m"] = waterbodies.geometry.apply(get_polygon_length)
    waterbodies = waterbodies[waterbodies.length_m <= (150 * 1000)]
    _log.info(
        f"Waterbodies count after filtering out waterbodies longer than than 150km: {len(waterbodies)}"
    )

    waterbodies["perim_m"] = waterbodies.geometry.length
    waterbodies = waterbodies.to_crs("EPSG:4326")

    # Assign unique ids to the waterbodies.
    waterbodies["uid"] = waterbodies.geometry.apply(
        lambda x: gh.encode(x.centroid.y, x.centroid.x, precision=10)
    )
    assert waterbodies["uid"].is_unique
    waterbodies.sort_values(by=["uid"], inplace=True)
    waterbodies.reset_index(inplace=True, drop=True)
    waterbodies["wb_id"] = waterbodies.index + 1
    assert waterbodies["wb_id"].min() > 0
    _log.info(f"Final waterbodies count: {len(waterbodies)}")

    # Write the polygons to the database.
    engine = get_waterbodies_engine()
    add_waterbodies_polygons_to_db(
        waterbodies_polygons=waterbodies, engine=engine, update_rows=True
    )
