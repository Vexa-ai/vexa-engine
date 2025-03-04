"""add_external_id_columns_to_content

Revision ID: 9fb2c48bb38c
Revises: 16237b9f460b
Create Date: 2025-02-19 20:15:09.294372

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9fb2c48bb38c'
down_revision: Union[str, None] = '16237b9f460b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add external_id and external_id_type columns
    op.add_column('content', sa.Column('external_id', sa.String(), nullable=True))
    op.add_column('content', sa.Column('external_id_type', sa.String(), nullable=True, server_default='none'))
    
    # Add indexes for the new columns
    op.create_index('idx_content_external_id', 'content', ['external_id'])
    op.create_index('idx_content_external_id_type', 'content', ['external_id_type'])
    
    # Add constraint for valid external_id_type values
    op.create_check_constraint(
        'valid_external_id_type',
        'content',
        "external_id_type IN ('google_meet', 'none')"
    )


def downgrade() -> None:
    # Remove indexes first
    op.drop_index('idx_content_external_id')
    op.drop_index('idx_content_external_id_type')
    
    # Remove constraint
    op.drop_constraint('valid_external_id_type', 'content')
    
    # Remove columns
    op.drop_column('content', 'external_id_type')
    op.drop_column('content', 'external_id')
