import logging
import os

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db_models import WaterbodyBase, WaterbodyHistoricalExtent
from waterbodies.io import check_file_exists, load_vector_file

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


def create_waterbodies_historical_extent(engine: Engine) -> Table:
    table = create_table(engine=engine, db_model=WaterbodyHistoricalExtent)
    return table


def delete_table(engine: Engine, table_name: str):
    table = get_existing_table(engine=engine, table_name=table_name)
    METADATA_OBJ.drop_all(bind=engine, tables=[table], checkfirst=True)


def add_waterbody_polygons_to_db(
    engine: Engine,
    waterbodies_polygons_fp: str,
    update_rows: bool = True,
):
    """
    Add the waterbodies polygons into the
    waterbodies historical extent  table.

    Parameters
    ----------
    engine : Engine
    update_rows : bool, optional
        If True if the polygon uid already exists in the waterbodies historical extent table,
        the row will be updated, else it will be skipped.
    waterbodies_polygons_fp : str
        Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons.
    """
    # connect to the db
    if not engine:
        engine = get_prod_waterbodies_engine()

    if not check_file_exists(path=waterbodies_polygons_fp):
        e = FileNotFoundError(f"File {waterbodies_polygons_fp} does not exist!)")
        _log.error(e)
        raise e
    else:
        try:
            waterbodies_polygons = load_vector_file(path=waterbodies_polygons_fp).to_crs(
                "EPSG:4326"
            )
        except Exception as error:
            _log.exception(error)
            raise error
