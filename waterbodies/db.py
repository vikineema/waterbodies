import logging
import os

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db_models import WaterbodyBase

_log = logging.getLogger(__name__)


METADATA_OBJ = WaterbodyBase.metadata


def get_dev_waterbodies_engine(password: str) -> Engine:
    """
    Create engine to connect to the DEV waterbodies database.
    For use in the DEV Analysis Sandbox.

    Parameters
    ----------
    password : str
        Password for the DEV waterbodies database.

    Returns
    -------
    Engine
        Engine to connect to the DEV waterbodies database.
    """
    dialect = "postgresql"
    driver = "psycopg2"
    username = "waterbodies_writer"
    host = "db-writer"
    port = 5432
    database_name = "waterbodies"

    database_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database_name}"
    return create_engine(database_url, future=True)


def get_prod_waterbodies_engine() -> Engine:
    """
    Create engine to connect to the PROD waterbodies database.

    Returns
    -------
    Engine
        Engine to connect to the PROD waterbodies database.
    """

    dialect = "postgresql"
    driver = "psycopg2"

    username = os.environ.get("WATERBODIES_DB_USER")
    password = os.environ.get("WATERBODIES_DB_PASS")
    host = os.environ.get("WATERBODIES_DB_HOST", "localhost")
    port = os.environ.get("WATERBODIES_DB_PORT", 5432)
    database_name = os.environ.get("WATERBODIES_DB_NAME")

    database_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database_name}"
    return create_engine(database_url, future=True)


def get_existing_table_names(engine: Engine) -> list[str]:
    """Get a list of the names of the tables that exist
    in the database.

    Parameters
    ----------
    engine : Engine
        Engine to connect to the database with.

    Returns
    -------
    list[str]
        List of the names of the tables in the database.
    """
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    table_names = [table.name for table in metadata_obj.sorted_tables]
    return table_names


def get_existing_table(engine: Engine, table_name: str) -> Table:
    """
    Get a table from the database.

    Parameters
    ----------
    engine : Engine
        Engine to connect to the database with.
    table_name : str
        Name of the table to get from the database.

    Returns
    -------
    Table
        Table object whose name matches the `table_name`.
    """
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    if table_name in metadata_obj.tables:
        return metadata_obj.tables[table_name]
    else:
        raise ValueError(f"Table '{table_name}' does not exist in the database.")


def create_table(engine: Engine, db_model) -> Table:
    """Create a table in the database from a mapped class.

    Parameters
    ----------
    engine : Engine
        Engine to connect to the database with.
    db_model:
        an ORM mapped class.

    Returns
    -------
    Table
        Table object with schema matching the mapped class.
    """
    METADATA_OBJ.create_all(bind=engine, tables=[db_model.__table__], checkfirst=True)
    table = get_existing_table(engine=engine, table_name=db_model.__table__.name)
    return table


def delete_table(engine: Engine, table_name: str):
    table = get_existing_table(engine=engine, table_name=table_name)
    METADATA_OBJ.drop_all(bind=engine, tables=[table], checkfirst=True)


def get_date_of_last_update(table_name: str, engine: str | None = None):

    if engine is None:
        engine = get_prod_waterbodies_engine()

    Session = sessionmaker(engine)

    with Session.begin() as session:
        result = session.execute(
            f"SELECT last_vacuum, last_autovacuum, last_analyze, \
                                 last_autoanalyze FROM pg_stat_user_tables WHERE relname \
                                 = '{table_name}'"
        )
        update_time_postgres = result.scalar()
    return update_time_postgres
