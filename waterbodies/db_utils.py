"""
Scripts run to edit/update the waterbodies tables
"""

from sqlalchemy import func, sessionmaker, update

from waterbodies.db import get_existing_table, get_waterbodies_engine


def update_waterbody_observations_task_id():
    engine = get_waterbodies_engine()
    table = get_existing_table(engine=engine, table_name="waterbody_observations")
    stmt = (
        update(table)
        .where(table.c.task_id == None)
        .values(task_id=func.split_part(table.c.obs_id, "_", 1))
    )
    Session = sessionmaker(bind=engine)
    with Session.begin() as session:
        session.execute(stmt)
