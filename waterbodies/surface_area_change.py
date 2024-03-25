from datetime import datetime

from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table, get_waterbodies_engine
from waterbodies.db_models import WaterbodyObservation


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


def get_last_observation_date(engine: Engine | None) -> datetime:
    """Get the date of the last waterbody observation.

    Parameters
    ----------
    engine : Engine | None

    Returns
    -------
    datetime
        Date of the last waterbody observation.
    """
    if engine is None:
        engine = get_waterbodies_engine()

    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(engine)
    with Session.begin() as session:
        last_observation_date = session.query(func.max(table.c.date)).scalar()

    return last_observation_date
