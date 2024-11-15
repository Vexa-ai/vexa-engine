"""merge_multiple_heads

Revision ID: 9007d4af72f0
Revises: 120a5a92fff0, 20241029_default_access, 38436906d17f
Create Date: 2024-10-29 18:55:34.089541

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9007d4af72f0'
down_revision: Union[str, None] = ('120a5a92fff0', '20241029_default_access', '38436906d17f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass