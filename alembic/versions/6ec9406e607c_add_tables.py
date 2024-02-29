"""Add tables

Revision ID: 6ec9406e607c
Revises: 9de3f1ea9e3f
Create Date: 2024-02-27 16:45:27.761385

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6ec9406e607c'
down_revision: Union[str, None] = '9de3f1ea9e3f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
