import logging
import os

import click
import geopandas as gpd
import numpy as np
import rioxarray  # noqa F401
from datacube import Datacube
from odc.geo.xr import to_cog
from tqdm import tqdm

from waterbodies.grid import WaterbodiesGrid
from waterbodies.hopper import create_tasks_from_datasets
from waterbodies.io import (
    check_directory_exists,
    get_filesystem,
    is_s3_path,
    load_vector_file,
)
from waterbodies.logs import logging_setup
from waterbodies.text import get_tile_index_str_from_tuple
from waterbodies.utils import rio_slurp_xarray


@click.command(
    name="split-hydrosheds-land-mask",
    help="Split the HydroSHEDS version 1.1 Land Mask into Tiles.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--goas-file-path",
    type=str,
    help="File path to the Global Oceans and Seas version 1 shapefile.",
)
@click.option(
    "--hydrosheds-land-mask-file-path",
    type=str,
    help="File path to the HydroSHEDS version 1.1 Land Mask GeoTIFF file.",
)
@click.option(
    "--output-directory",
    type=str,
    help="Directory to write the land/sea mask tiles to.",
)
def split_hydrosheds_land_mask(
    verbose,
    goas_file_path,
    hydrosheds_land_mask_file_path,
    output_directory,
):

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(output_directory):
        fs = get_filesystem(output_directory, anon=False)
        fs.mkdirs(output_directory)
        _log.info(f"Created directory {output_directory}")

    if is_s3_path(output_directory):
        # To avoid the error GDAL signalled an error: err_no=1, msg='w+ not supported for /vsis3,
        # unless CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE is set to YES'
        # when writing to s3 using rioxarray's rio.to_raster
        os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"

    # Find all the tiles that will be used to generate the Waterbodies
    # historical extent polygons
    dc = Datacube(app="tiles")
    gridspec = WaterbodiesGrid().gridspec

    dc_query = dict(product="wofs_ls_summary_alltime")
    datasets = dc.find_datasets(**dc_query)
    tasks = create_tasks_from_datasets(
        datasets=datasets, tile_index_filter=None, bin_solar_day=False
    )
    tile_indices = [k for task in tasks for k, v in task.items()]
    tile_extents = [
        gridspec.tile_geobox(tile_index=tile_index).extent.geom for tile_index in tile_indices
    ]
    tile_extents_gdf = gpd.GeoDataFrame(
        data={"tile_index": tile_indices, "geometry": tile_extents}, crs=gridspec.crs
    )
    tile_extents_gdf.set_index("tile_index", inplace=True)

    _log.info(f"Found {len(tile_extents_gdf)} WaterBodiesGrid tiles")

    # Load the Global Oceans and Seas dataset.
    goas_v01_gdf = load_vector_file(goas_file_path).to_crs(gridspec.crs)

    # Identify all tiles that intersect with  Global Oceans and Seas dataset
    # This will be the coastal tiles.
    coastal_tile_indices = (
        tile_extents_gdf.sjoin(goas_v01_gdf, predicate="intersects", how="inner")
        .index.unique()
        .to_list()
    )
    coastal_tile_geoboxes = [
        gridspec.tile_geobox(tile_index=tile_index) for tile_index in coastal_tile_indices
    ]
    coastal_tiles = list(zip(coastal_tile_indices, coastal_tile_geoboxes))

    _log.info(f"Found {len(coastal_tiles)} coastal WaterBodiesGrid tiles")

    fs = get_filesystem(output_directory, anon=False)
    with tqdm(
        iterable=coastal_tiles,
        desc="Rasterizing coastal HydroSHEDS version 1.1 Land Mask tiles",
        total=len(coastal_tiles),
    ) as coastal_tiles:
        for tile in coastal_tiles:
            tile_index, tile_geobox = tile
            tile_index_str = get_tile_index_str_from_tuple(tile_index)
            tile_raster_fp = os.path.join(
                output_directory, f"hydrosheds_v1_1_land_mask_{tile_index_str}.tif"
            )
            tile_hydrosheds_land_mask = rio_slurp_xarray(
                fname=hydrosheds_land_mask_file_path, gbox=tile_geobox, resampling="bilinear"
            )
            # Indicator values: 1 = land, 2 = ocean sink, 3 = inland sink, 255 is no data.
            tile_raster = np.logical_or(
                tile_hydrosheds_land_mask == 1, tile_hydrosheds_land_mask == 3
            ).astype(np.int32)
            # Compress xarray.DataArray into Cloud Optimized GeoTiff bytes in memory.
            cog_bytes = to_cog(geo_im=tile_raster)
            # Write to file
            with fs.open(tile_raster_fp, "wb") as file:
                file.write(cog_bytes)
