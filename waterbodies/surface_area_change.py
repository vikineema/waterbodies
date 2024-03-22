from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import Table

from waterbodies.db import (
    create_table,
    get_date_of_last_update,
    get_prod_waterbodies_engine,
)
from waterbodies.db_models import WaterbodyObservation


def create_waterbodies_observations_table(engine: Engine) -> Table:
    table = create_table(engine=engine, db_model=WaterbodyObservation)
    return table


def get_last_water_observations_table_update(engine: Engine | None):
    if engine is None:
        engine = get_prod_waterbodies_engine()

    table = create_waterbodies_observations_table(engine=engine)
    table_name = table.name

    last_update_date = get_date_of_last_update(table_name=table_name, engine=engine)

    return last_update_date
