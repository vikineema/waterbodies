import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from geoalchemy2 import load_spatialite
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.event import listen
from sqlalchemy.schema import Table

from waterbodies.db_models import WaterbodyBase
from waterbodies.io import check_file_exists

_log = logging.getLogger(__name__)


def is_sandbox_env() -> bool:
    """
    Check if running on the Analysis Sandbox

    Returns
    -------
    bool
        True if on Sandbox
    """
    return bool(os.environ.get("JUPYTERHUB_USER", None))


def check_waterbodies_db_credentials_exist() -> bool:
    return bool(os.environ.get("WATERBODIES_DB_USER", None))


def setup_sandbox_env(dotenv_path: str = os.path.join(str(Path.home()), ".env")):
    """
    Load the .env file to set up the waterbodies database
    credentials on the Analysis Sandbox.

    Parameters
    ----------
    dotenv_path : str, optional
        Absolute or relative path to .env file, by default os.path.join(str(Path.home()), ".env")
    """

    if not check_waterbodies_db_credentials_exist():
        check_dotenv = load_dotenv(dotenv_path=dotenv_path, verbose=True, override=True)
        if not check_dotenv:
            # Check if the file does not exist
            if not check_file_exists(dotenv_path):
                e = FileNotFoundError(f"{dotenv_path} does NOT exist!")
                _log.exception(e)
                raise e
            else:
                e = ValueError(f"No variables found in {dotenv_path}")
                _log.exception(e)
                raise e
        else:
            if not check_waterbodies_db_credentials_exist():
                raise ValueError(f"Waterbodies database credentials not in {dotenv_path}")


def get_test_waterbodies_engine() -> Engine:
    """Get a SQLite in-memory database engine."""

    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
    listen(engine, "connect", load_spatialite)
    # Create the required waterbodies tables in the engine
    metadata_obj = WaterbodyBase.metadata
    metadata_obj.create_all(bind=engine, checkfirst=True)
    return engine


def get_main_waterbodies_engine() -> Engine:
    """
    Create engine to connect to the DEV/PROD waterbodies database.

    Returns
    -------
    Engine
        Engine to connect to the DEV/PROD waterbodies database.
    """
    if is_sandbox_env():
        setup_sandbox_env()

    dialect = "postgresql"
    driver = "psycopg2"

    username = os.environ.get("WATERBODIES_DB_USER")
    password = os.environ.get("WATERBODIES_DB_PASS")
    host = os.environ.get("WATERBODIES_DB_HOST", "localhost")
    port = os.environ.get("WATERBODIES_DB_PORT", 5432)
    database_name = os.environ.get("WATERBODIES_DB_NAME")

    database_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database_name}"
    return create_engine(database_url, future=True)


def check_testing_mode() -> bool:
    return bool(os.environ.get("TestingMode", None))


def get_waterbodies_engine() -> Engine:
    """
    Get the waterbodies engine to connect to.

    Returns
    -------
    Engine
        Waterbodies engine
    """
    if check_testing_mode():
        engine = get_test_waterbodies_engine()
    else:
        engine = get_main_waterbodies_engine()
    return engine


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
    metadata_obj = WaterbodyBase.metadata
    metadata_obj.create_all(bind=engine, tables=[db_model.__table__], checkfirst=True)
    table = get_existing_table(engine=engine, table_name=db_model.__table__.name)
    return table


def delete_table(engine: Engine, table_name: str):
    """
    Delete a table in the database.

    Parameters
    ----------
    engine : Engine
    table_name : str
        Name of the table to delete.
    """
    table = get_existing_table(engine=engine, table_name=table_name)
    metadata_obj = WaterbodyBase.metadata
    metadata_obj.drop_all(bind=engine, tables=[table], checkfirst=True)
