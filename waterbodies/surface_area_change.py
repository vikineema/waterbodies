import datetime
import logging

from datacube import Datacube
from datacube.model import Dataset
from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table
from waterbodies.db_models import WaterbodyObservation
from waterbodies.hopper import find_datasets_by_creation_date

_log = logging.getLogger(__name__)


def create_waterbodies_observations_table(engine: Engine) -> Table:
    """
    Create the waterbodies_observations table if it does not exist.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    Table
        The waterbodies_observations table.
    """
    table = create_table(engine=engine, db_model=WaterbodyObservation)
    return table


def get_last_waterbody_observation_date(engine: Engine) -> datetime:
    """
    Get the date of the last waterbody observation.

    Parametersrom odc.dscache.tools.tiling import parse_gridspec_with_name
    from odc.dscache.tools import bin_dataset_stream
    from waterbodies.hopper import persist     Date of the last waterbody observation.
    ----------
    engine : Engine | None

    Returns
    -------
    datetime
        Date of the last waterbody observation
    """
    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(engine)
    with Session.begin() as session:
        last_observation_date = session.query(func.max(table.c.date)).scalar()

    return last_observation_date


def get_datasets_for_gapfill(engine: Engine) -> list[Dataset]:
    """
    Get all the wofs_ls scenes added to the datacube since the
    date of the last waterbody observation to today.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    list[Dataset]
        All the wofs_ls scenes added to the datacube since the date of the
        last waterbody observation to today.
    """

    dc = Datacube(app="gapfill")

    last_observation_date = get_last_waterbody_observation_date(engine=engine).date()

    today = datetime.datetime.now().date()

    dss = find_datasets_by_creation_date(
        product="wofs_ls", start_date=last_observation_date, end_date=today, dc=dc
    )

    _log.found(
        f'Found {len(dss)} new wofs_ls scenes added \
        to the datacube since {last_observation_date.strftime("%Y-%m-%d")}'
    )
    return dss
