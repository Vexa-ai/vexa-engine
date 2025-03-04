"""add_external_id_unique_constraint

Revision ID: a1b2c3d4e5f6
Revises: 9fb2c48bb38c
Create Date: 2025-02-20 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '9fb2c48bb38c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create unique constraint for external_id and external_id_type
    op.create_unique_constraint(
        'uq_content_external_id',
        'content',
        ['external_id', 'external_id_type']
    )


def downgrade() -> None:
    # Remove unique constraint
    op.drop_constraint('uq_content_external_id', 'content') 