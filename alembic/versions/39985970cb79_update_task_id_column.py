"""Update task id column

Revision ID: 39985970cb79
Revises: da96f386855c
Create Date: 2024-04-08 18:53:27.716036

"""

from typing import Sequence, Union

from sqlalchemy import Column, ForeignKey, String, Table, func, update
from sqlalchemy.orm import sessionmaker

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "39985970cb79"
down_revision: Union[str, None] = "da96f386855c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# From https://stackoverflow.com/a/24623979
# Only need to define the priamry key and the columns used.
table = Table(
    "waterbody_observations",
    Column("obs_id", String, primary_key=True),
    Column("uid", String, ForeignKey("waterbodies_historical_extent.uid"), index=True),
    Column("task_id", String),
)


def upgrade() -> None:
    bind = op.get_bind()
    Session = sessionmaker(bind=bind)

    with Session.begin() as session:
        stmt = (
            update(table)
            .where(table.c.task_id == None)
            .values(task_id=func.split_part(table.c.obs_id, "_", 1))
        )
        session.execute(stmt)


def downgrade() -> None:
    bind = op.get_bind()
    Session = sessionmaker(bind=bind)
    with Session.begin() as session:
        stmt = (update(table).values(task_id=None))
        session.execute(stmt)
