from datacube.testutils.io import rio_slurp_xarray as dc_rio_slurp_xarray
from datacube.utils.geometry import GeoBox as dc_Geobox
from odc.geo.geobox import GeoBox
from odc.geo.xr import wrap_xr  # noqa F401


# Wrapper around datacube rio_slurp_xarray
# because of difference in behaviour between datacube.utils.geometry.Geobox
# and odc.geo.geobox.GeoBox
def rio_slurp_xarray(fname, *args, rgb="auto", **kw):
    if len(args) == 0:
        if "gbox" in kw:
            gbox = kw["gbox"]
            if isinstance(gbox, GeoBox):
                kw.update(
                    {
                        "gbox": dc_Geobox(
                            width=gbox.width, height=gbox.height, affine=gbox.affine, crs=gbox.crs
                        )
                    }
                )
    else:
        if isinstance(args[0], GeoBox):
            gbox = args[0]
            args[0] = dc_Geobox(
                width=gbox.width, height=gbox.height, affine=gbox.affine, crs=gbox.crs
            )

    return dc_rio_slurp_xarray(fname, *args, rgb=rgb, **kw)
