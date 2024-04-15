from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, declarative_base

WaterbodyBase = declarative_base()


class WaterbodyHistoricalExtent(WaterbodyBase):
    __tablename__ = "waterbodies_historical_extent"

    uid: Mapped[str] = Column(String, primary_key=True)
    wb_id: Mapped[int] = Column(Integer)
    area_m2: Mapped[float] = Column(Float)
    length_m: Mapped[float] = Column(Float)
    perim_m: Mapped[float] = Column(Float)
    timeseries: Mapped[str] = Column(String)
    geometry = Column(Geometry(geometry_type="POLYGON"))

    def __repr__(self) -> str:
        return f"WaterbodyHistoricalExtent(uid={self.uid!r}, wb_id={self.wb_id!r}, ...)"


class WaterbodyObservation(WaterbodyBase):
    __tablename__ = "waterbodies_observations_test"

    obs_id: Mapped[str] = Column(String, primary_key=True)
    uid: Mapped[str] = Column(String, ForeignKey("waterbodies_historical_extent.uid"), index=True)
    px_total: Mapped[int] = Column(Integer)
    px_wet: Mapped[int] = Column(Integer)
    area_wet_m2: Mapped[float] = Column(Float)
    px_dry: Mapped[int] = Column(Integer)
    area_dry_m2: Mapped[float] = Column(Float)
    px_invalid: Mapped[int] = Column(Integer)
    area_invalid_m2: Mapped[float] = Column(Float)
    date: Mapped[datetime] = Column(Date)
    task_id: Mapped[str] = Column(String)

    def __repr__(self) -> str:
        return (
            f"<WaterbodyObservation obs_id={self.obs_id}, uid={self.uid}, "
            + f"date={self.date}, ...>"
        )
