import logging

import geopandas as gpd
from sqlalchemy import insert, select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

from waterbodies.db import create_table, get_waterbodies_engine
from waterbodies.db_models import WaterbodyHistoricalExtent
from waterbodies.io import check_file_exists, load_vector_file

_log = logging.getLogger(__name__)


def validate_waterbodies_polygons(waterbodies_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Validate the waterbodies_polygons GeoDataFrame to ensure it's
    structure is as expected.

    Parameters
    ----------
    waterbodies_polygons : gpd.GeoDataFrame
        The waterbodies polygons

    Returns
    -------
    gpd.GeoDataFrame
        The waterbodies polygons if all assertions passed.
    """

    assert all([col in waterbodies_polygons.columns for col in ["UID", "WB_ID"]])

    assert waterbodies_polygons["UID"].is_unique

    assert waterbodies_polygons["WB_ID"].is_unique
    assert waterbodies_polygons["WB_ID"].min() > 0

    return waterbodies_polygons


def create_waterbodies_historical_extent_table(engine: Engine) -> Table:
    """
    Create the waterbodies_historical_extent_table if
    it does not exist.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    Table
        waterbodies_historical_extent table
    """
    table = create_table(engine=engine, db_model=WaterbodyHistoricalExtent)
    return table


def add_waterbodies_polygons_to_db(
    waterbodies_polygons_file_path: str,
    engine: Engine,
    update_rows: bool = True,
):
    """
    Add the waterbodies polygons to the waterbodies
    historical exent table.

    Parameters
    ----------
    waterbodies_polygons_file_path : str
        Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons.
    engine : Engine
    update_rows : bool, optional
         If True if the polygon uid already exists in the waterbodies table, the row will be
         updated else it will be skipped, by default True

    """
    if not check_file_exists(path=waterbodies_polygons_file_path):
        e = FileNotFoundError(f"File {waterbodies_polygons_file_path} does not exist!)")
        _log.error(e)
        raise e
    else:
        try:
            waterbodies_polygons = load_vector_file(path=waterbodies_polygons_file_path).to_crs(
                "EPSG:4326"
            )
        except Exception as error:
            _log.exception(error)
            raise error

    waterbodies_polygons = validate_waterbodies_polygons(waterbodies_polygons)

    # Ensure historical extent table exists
    table = create_waterbodies_historical_extent_table(engine=engine)

    Session = sessionmaker(bind=engine)

    update_statements = []
    insert_parameters = []

    with Session.begin() as session:
        uids = session.scalars(select(table.c["uid"])).all()
        _log.info(f"Found {len(uids)} polygon UIDs in the {table.name} table")

    srid = waterbodies_polygons.crs.to_epsg()

    for row in waterbodies_polygons.itertuples():
        if row.UID not in uids:
            insert_parameters.append(
                dict(
                    uid=row.UID,
                    area_m2=row.area_m2,
                    wb_id=row.WB_ID,
                    length_m=row.length_m,
                    perim_m=row.perim_m,
                    timeseries=row.timeseries,
                    geometry=f"SRID={srid};{row.geometry.wkt}",
                )
            )
        else:
            if update_rows:
                update_statements.append(
                    update(table)
                    .where(table.c.uid == row.UID)
                    .values(
                        dict(
                            area_m2=row.area_m2,
                            wb_id=row.WB_ID,
                            length_m=row.length_m,
                            perim_m=row.perim_m,
                            timeseries=row.timeseries,
                            geometry=f"SRID={srid};{row.geometry.wkt}",
                        )
                    )
                )
            else:
                continue

    if update_statements:
        _log.info(f"Updating {len(update_statements)} polygons in the {table.name} table")
        with Session.begin() as session:
            for statement in update_statements:
                session.execute(statement)
    else:
        _log.info(f"No polygons to update in the {table.name} table")

    if insert_parameters:
        _log.info(f"Adding {len(insert_parameters)} polygons to the {table.name} table")
        with Session.begin() as session:
            session.execute(insert(table), insert_parameters)
    else:
        _log.error(f"No polygons to insert into the {table.name} table")


def load_waterbodies_from_db(engine: Engine) -> gpd.GeoDataFrame:
    """
    Load all waterbodies polygons from the `waterbodies_historical_extent`
    table.

    Parameters
    ----------
    engine : Engine

    Returns
    -------
    gpd.GeoDataFrame
        All waterbodies polygons present in the `waterbodies_historical_extent` table.
    """

    table = create_waterbodies_historical_extent_table(engine=engine)
    table_name = table.name

    sql_query = f"SELECT * FROM {table_name}"

    waterbodies = gpd.read_postgis(sql_query, engine, geom_col="geometry")

    return waterbodies
