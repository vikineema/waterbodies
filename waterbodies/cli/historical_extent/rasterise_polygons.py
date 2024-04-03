import json
import logging
import os

import click
import geopandas as gpd
import rioxarray  # noqa F401
from odc.geo.geom import Geometry
from odc.geo.xr import wrap_xr
from rasterio.features import rasterize
from tqdm import tqdm

from waterbodies.grid import WaterbodiesGrid
from waterbodies.historical_extent import validate_waterbodies_polygons
from waterbodies.io import (
    check_directory_exists,
    get_filesystem,
    is_s3_path,
    load_vector_file,
)
from waterbodies.logs import logging_setup
from waterbodies.text import tile_id_tuple_to_str


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
@click.option(
    "--historical-extent-vector-file",
    type=str,
    help="Path to the historical extent polygons vector file.",
)
def rasterise_polygons(
    verbose,
    historical_extent_rasters_directory,
    historical_extent_vector_file,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    if not check_directory_exists(historical_extent_rasters_directory):
        fs = get_filesystem(historical_extent_rasters_directory)
        fs.mkdirs(historical_extent_rasters_directory)
        _log.info(f"Created directory {historical_extent_rasters_directory}")

    if is_s3_path(historical_extent_rasters_directory):
        # To avoid the error GDAL signalled an error: err_no=1, msg='w+ not supported for /vsis3,
        # unless CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE is set to YES'
        # when writing to s3 using rioxarray's rio.to_raster
        os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"

    gridspec = WaterbodiesGrid().gridspec

    product_footprint = gpd.read_file(
        "https://explorer.digitalearth.africa/api/footprint/wofs_ls_summary_alltime"
    ).to_crs(gridspec.crs)
    product_footprint_geopolygon = Geometry(
        geom=product_footprint.geometry.iloc[0], crs=gridspec.crs
    )
    tiles = gridspec.tiles_from_geopolygon(geopolygon=product_footprint_geopolygon)
    tiles = list(tiles)

    historical_extent_polygons = load_vector_file(path=historical_extent_vector_file)
    historical_extent_polygons = validate_waterbodies_polygons(
        waterbodies_polygons=historical_extent_polygons
    )
    historical_extent_polygons = historical_extent_polygons.to_crs(gridspec.crs)
    historical_extent_polygons.set_index("WB_ID", inplace=True)

    with tqdm(
        iterable=tiles, desc="Rasterise historical extent polygons by grid tile", total=len(tiles)
    ) as tiles:
        for tile in tiles:
            tile_id, tile_geobox = tile

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
                # Rasterize the intersecting historical extent polygons using the WB_ID for the
                # polygon as the pixel value.
                intersecting_polygons = historical_extent_polygons[
                    historical_extent_polygons.index.isin(intersecting_polygons_ids)
                ]
                shapes = zip(intersecting_polygons.geometry, intersecting_polygons.index)
                tile_raster_np = rasterize(
                    shapes=shapes, out_shape=tile_geobox.shape, transform=tile_geobox.transform
                )
                tile_raster_ds = wrap_xr(im=tile_raster_np, gbox=tile_geobox)
                # Add a dictionary mapping the WB_ID values to the UID values as part of the
                # metadata of the raster.
                tags = dict(
                    WB_ID_to_UID=json.dumps(
                        dict(zip(intersecting_polygons.index, intersecting_polygons.UID))
                    )
                )
                # Write the raster to file.
                raster_path = os.path.join(
                    historical_extent_rasters_directory, f"{tile_id_tuple_to_str(tile_id)}.tif"
                )
                tile_raster_ds.rio.to_raster(raster_path=raster_path, tags=tags, compute=True)
