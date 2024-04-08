"""Add task_id column

Revision ID: da96f386855c
Revises: 
Create Date: 2024-04-08 18:23:50.043734

"""

from typing import Sequence, Union

from sqlalchemy import Column, String

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "da96f386855c"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("waterbody_observations") as batch_op:
        batch_op.add_column(Column("task_id", String))


def downgrade() -> None:
    with op.batch_alter_table("waterbody_observations") as batch_op:
        batch_op.drop_column("task_id")
