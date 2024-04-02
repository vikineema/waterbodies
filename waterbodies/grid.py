from odc.geo import Resolution
from odc.geo.gridspec import GridSpec


class WaterbodiesGrid:
    def __init__(self):
        self.grid_name = "waterbodies_grid"
        self.grid_spec = GridSpec(
            crs="EPSG:6933", tile_shape=(96000.0, 96000.0), resolution=Resolution(x=30, y=-30)
        )
