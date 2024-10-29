"""merge_heads

Revision ID: 0334bafca1e2
Revises: 9007d4af72f0
Create Date: 2024-10-29 18:56:20.471817

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0334bafca1e2'
down_revision: Union[str, None] = '9007d4af72f0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass