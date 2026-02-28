import json
import logging
import os

import click
import geopandas as gpd
import rioxarray  # noqa F401
from datacube import Datacube
from odc.geo.xr import to_cog, wrap_xr
from rasterio.features import rasterize
from tqdm import tqdm

from waterbodies.db import get_waterbodies_engine
from waterbodies.grid import WaterbodiesGrid
from waterbodies.historical_extent import (
    load_waterbodies_from_db,
    validate_waterbodies_polygons,
)
from waterbodies.hopper import create_tasks_from_datasets
from waterbodies.io import check_directory_exists, get_filesystem, is_s3_path
from waterbodies.logs import logging_setup
from waterbodies.text import get_tile_index_str_from_tuple


@click.command(
    name="rasterise-polygons",
    help="Rasterise historical extent polygons by tile.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--historical-extent-rasters-directory",
    type=str,
    help="Path of the directory to write the historical extent raster files to .",
)
def rasterise_polygons(
    verbose,
    historical_extent_rasters_directory,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(historical_extent_rasters_directory):
        fs = get_filesystem(historical_extent_rasters_directory, anon=False)
        fs.mkdirs(historical_extent_rasters_directory)
        _log.info(f"Created directory {historical_extent_rasters_directory}")

    if is_s3_path(historical_extent_rasters_directory):
        # To avoid the error GDAL signalled an error: err_no=1, msg='w+ not supported for /vsis3,
        # unless CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE is set to YES'
        # when writing to s3 using rioxarray's rio.to_raster
        os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"

    gridspec = WaterbodiesGrid().gridspec

    # Get all the tiles used to generate the waterbodies.
    dc = Datacube(app="RasteriseWaterbodies")
    datasets = dc.find_datasets(product="wofs_ls_summary_alltime")
    tasks = create_tasks_from_datasets(
        datasets=datasets, tile_index_filter=None, bin_solar_day=False
    )
    tile_indices = [k for task in tasks for k, v in task.items()]
    tiles = [
        (tile_index, gridspec.tile_geobox(tile_index=tile_index)) for tile_index in tile_indices
    ]

    # Load the historical extent polygons.
    engine = get_waterbodies_engine()
    historical_extent_polygons = load_waterbodies_from_db(engine=engine)
    historical_extent_polygons = validate_waterbodies_polygons(
        waterbodies_polygons=historical_extent_polygons
    )
    historical_extent_polygons = historical_extent_polygons.to_crs(gridspec.crs)
    historical_extent_polygons.set_index("wb_id", inplace=True)

    fs = get_filesystem(historical_extent_rasters_directory, anon=False)
    with tqdm(
        iterable=tiles, desc="Rasterise historical extent polygons by grid tile", total=len(tiles)
    ) as tiles:
        for tile in tiles:
            tile_index, tile_geobox = tile

            # Get the historical extent polygons that intersect with the extent of the tile's
            # Geobox.
            tile_geobox_extent = gpd.GeoDataFrame(
                geometry=[tile_geobox.extent.geom], crs=tile_geobox.extent.crs
            )
            intersecting_polygons_ids = gpd.sjoin(
                historical_extent_polygons, tile_geobox_extent, how="inner", predicate="intersects"
            ).index.to_list()

            if not intersecting_polygons_ids:
                continue
            else:
                # Rasterize the intersecting historical extent polygons using the wb_id for the
                # polygon as the pixel value.
                intersecting_polygons = historical_extent_polygons[
                    historical_extent_polygons.index.isin(intersecting_polygons_ids)
                ]
                shapes = zip(intersecting_polygons.geometry, intersecting_polygons.index)
                tile_raster_np = rasterize(
                    shapes=shapes, out_shape=tile_geobox.shape, transform=tile_geobox.transform
                )
                tile_raster_da = wrap_xr(im=tile_raster_np, gbox=tile_geobox)
                # Add a dictionary mapping the wb_id values to the uid values as part of the
                # metadata of the raster.
                tags = dict(
                    WB_ID_to_UID=json.dumps(
                        dict(zip(intersecting_polygons.index, intersecting_polygons.uid))
                    )
                )
                # Write the raster to file.
                raster_path = os.path.join(
                    historical_extent_rasters_directory,
                    f"{get_tile_index_str_from_tuple(tile_index)}.tif",
                )
                # Compress xarray.DataArray into Cloud Optimized GeoTiff bytes in memory.
                cog_bytes = to_cog(geo_im=tile_raster_da, tags=tags)
                # Write to file
                with fs.open(raster_path, "wb") as file:
                    file.write(cog_bytes)
