"""add_note_type_to_content

Revision ID: add_note_type_to_content
Revises: d005f42e37d2
Create Date: 2024-03-21 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'add_note_type_to_content'
down_revision: Union[str, None] = 'd005f42e37d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Add NOTE to the content type check constraint
    op.drop_constraint('content_type_check', 'content', type_='check')
    op.create_check_constraint(
        'content_type_check',
        'content',
        "type IN ('meeting', 'document', 'title', 'summary', 'chunk', 'note')"
    )

def downgrade() -> None:
    # Remove NOTE from the content type check constraint
    op.drop_constraint('content_type_check', 'content', type_='check')
    op.create_check_constraint(
        'content_type_check',
        'content',
        "type IN ('meeting', 'document', 'title', 'summary', 'chunk')"
    ) 