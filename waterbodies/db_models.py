from geoalchemy2 import Geometry
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class WaterbodyBase(DeclarativeBase):
    pass


class WaterbodyHistoricalExtent(WaterbodyBase):
    __tablename__ = "waterbodies_historical_extent"

    uid: Mapped[str] = mapped_column(primary_key=True)
    wb_id: Mapped[int]
    area_m2: Mapped[float]
    length_m: Mapped[float]
    perim_m: Mapped[float]
    timeseries: Mapped[str]
    geometry = mapped_column(Geometry(geometry_type="POLYGON"))

    def __repr__(self) -> str:
        return f"WaterbodyHistoricalExtent(uid={self.uid!r}, wb_id={self.wb_id!r}, ...)"
