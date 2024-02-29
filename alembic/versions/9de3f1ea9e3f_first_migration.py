"""First migration

Revision ID: 9de3f1ea9e3f
Revises: 68929fc327d0
Create Date: 2024-02-27 16:01:34.063214

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9de3f1ea9e3f'
down_revision: Union[str, None] = '68929fc327d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
