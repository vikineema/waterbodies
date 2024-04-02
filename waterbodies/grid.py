from odc.dscache.tools.tiling import parse_gridspec_with_name


class WaterbodiesGrid:
    def __init__(self):
        self.grid_name, self.gridspec = parse_gridspec_with_name("africa_30")
