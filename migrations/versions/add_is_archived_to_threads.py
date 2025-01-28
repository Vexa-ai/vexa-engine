"""add is_archived to threads

Revision ID: add_is_archived_to_threads
Revises: 
Create Date: 2024-01-28 11:20:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'add_is_archived_to_threads'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('threads', sa.Column('is_archived', sa.Boolean(), nullable=False, server_default='false'))

def downgrade():
    op.drop_column('threads', 'is_archived') 