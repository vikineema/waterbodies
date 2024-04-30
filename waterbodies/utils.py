from datacube.testutils.io import rio_slurp_read, rio_slurp_reproject
from odc.geo.geobox import GeoBox
from odc.geo.xr import wrap_xr


# Copied from
# https://github.com/opendatacube/datacube-core/blob/9d3c6c1e63a0e269ba6d9e95482a35eda0ea4ec9/datacube/testutils/io.py#L370C1-L404C44
# because of difference in behaviour between datacube.utils.geometry.Geobox
# and odc.geo.geobox.GeoBox
def rio_slurp_xarray(fname, *args, rgb="auto", **kw):
    """
    Dispatches to either:

    rio_slurp_read(fname, out_shape, ..)
    rio_slurp_reproject(fname, gbox, ...)

    then wraps it all in xarray.DataArray with .crs,.nodata etc.
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

    return wrap_xr(im=im, gbox=mm.gbox, **dict(nodata=mm.nodata))
