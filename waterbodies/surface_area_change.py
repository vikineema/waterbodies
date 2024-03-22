from sqlalchemy import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table, get_prod_waterbodies_engine
from waterbodies.db_models import WaterbodyObservation


def create_waterbodies_observations_table(engine: Engine) -> Table:
    table = create_table(engine=engine, db_model=WaterbodyObservation)
    return table


def get_most_recent_observation(engine: Engine | None):
    if engine is None:
        engine = get_prod_waterbodies_engine()

    table = create_waterbodies_observations_table(engine=engine)

    Session = sessionmaker(engine)
    with Session.begin() as session:
        most_recent_date = session.query(func.max(table.date)).scalar()

    return most_recent_date
