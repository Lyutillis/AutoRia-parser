"""First migration

Revision ID: 68929fc327d0
Revises: ef34fde076f4
Create Date: 2024-02-27 15:57:26.844116

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '68929fc327d0'
down_revision: Union[str, None] = 'ef34fde076f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
