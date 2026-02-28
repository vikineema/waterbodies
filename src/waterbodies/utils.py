from types import SimpleNamespace

import numpy as np
import rasterio
from datacube.testutils.io import _fix_resampling
from odc.geo import wh_
from odc.geo.crs import CRS
from odc.geo.geobox import GeoBox, zoom_to
from odc.geo.xr import xr_coords
from rasterio.warp import reproject
from xarray import DataArray

# Note: Functions here are adapted from the datacube.testutils.oi module because
# of differences in behaviour between odc.geobox.GeoBox and datacube.utils.geometry.Geobox


def rio_geobox(meta):
    """Construct geobox from src.meta of opened rasterio dataset"""
    if "crs" not in meta or "transform" not in meta:
        return None

    h, w = (meta["height"], meta["width"])
    crs = CRS(meta["crs"])
    transform = meta["transform"]

    return GeoBox(wh_(w, h), transform, crs)


def rio_slurp_reproject(fname, gbox, dtype=None, dst_nodata=None, **kw):
    """
    Read image with reprojection
    """

    _fix_resampling(kw)

    with rasterio.open(str(fname), "r") as src:
        if src.count == 1:
            shape = gbox.shape
            src_band = rasterio.band(src, 1)
        else:
            shape = (src.count, *gbox.shape)
            src_band = rasterio.band(src, tuple(range(1, src.count + 1)))

        if dtype is None:
            dtype = src.dtypes[0]
        if dst_nodata is None:
            dst_nodata = src.nodata
        if dst_nodata is None:
            dst_nodata = 0

        pix = np.full(shape, dst_nodata, dtype=dtype)

        reproject(
            src_band,
            pix,
            dst_nodata=dst_nodata,
            dst_transform=gbox.transform,
            dst_crs=str(gbox.crs),
            **kw,
        )

        meta = src.meta
        meta["src_gbox"] = rio_geobox(meta)
        meta["path"] = fname
        meta["gbox"] = gbox

        return pix, SimpleNamespace(**meta)


def rio_slurp_read(fname, out_shape=None, **kw):
    """
    Read whole image file using rasterio.

    :returns: ndarray (2d or 3d if multi-band), dict (rasterio meta)
    """
    _fix_resampling(kw)

    if out_shape is not None:
        kw.update(out_shape=out_shape)

    with rasterio.open(str(fname), "r") as src:
        data = src.read(1, **kw) if src.count == 1 else src.read(**kw)
        meta = src.meta
        src_gbox = rio_geobox(meta)

        same_gbox = out_shape is None or out_shape == src_gbox.shape
        gbox = src_gbox if same_gbox else zoom_to(src_gbox, out_shape)

        meta["src_gbox"] = src_gbox
        meta["gbox"] = gbox
        meta["path"] = fname
        return data, SimpleNamespace(**meta)


def rio_slurp_xarray(fname, *args, rgb="auto", **kw):
    """
    Dispatches to either:

    rio_slurp_read(fname, out_shape, ..)
    rio_slurp_reproject(fname, gbox, ...)

    then wraps it all in xarray.Dafrom xarray import DataArraytaArray with .crs,.nodata etc.
    """

    if len(args) == 0:
        if "gbox" in kw:
            im, mm = rio_slurp_reproject(fname, **kw)
        else:
            im, mm = rio_slurp_read(fname, **kw)
    else:
        if isinstance(args[0], GeoBox):
            im, mm = rio_slurp_reproject(fname, *args, **kw)
        else:
            im, mm = rio_slurp_read(fname, *args, **kw)

    if im.ndim == 3:
        dims = ("band", *mm.gbox.dims)
        if rgb and im.shape[0] in (3, 4):
            im = im.transpose([1, 2, 0])
            dims = tuple(dims[i] for i in [1, 2, 0])
    else:
        dims = mm.gbox.dims

    coords = xr_coords(mm.gbox, crs_coord_name="spatial_ref")

    return DataArray(im, dims=dims, coords=coords, attrs=dict(nodata=mm.nodata))


class TaskFailedError(Exception):
    """General exception for failed tasks."""

    def __init__(self, message="One or more tasks have failed."):
        super().__init__(message)
