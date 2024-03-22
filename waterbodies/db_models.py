from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, Mapped, mapped_column


WaterbodyBase = declarative_base()


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


class WaterbodyObservation(WaterbodyBase):
    __tablename__ = "waterbody_observations"

    obs_id: Mapped[str] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(ForeignKey("waterbodies_historical_extent.uid"), index=True)
    px_total: Mapped[int]
    px_wet: Mapped[float]
    area_wet_m2: Mapped[float]
    px_dry: Mapped[float]
    area_dry_m2: Mapped[float]
    px_invalid: Mapped[float]
    area_invalid_m2: Mapped[float]
    date: Mapped[datetime]

    def __repr__(self) -> str:
        return (
            f"<WaterbodyObservation obs_id={self.obs_id}, uid={self.uid}, "
            + f"date={self.date}, ...>"
        )
