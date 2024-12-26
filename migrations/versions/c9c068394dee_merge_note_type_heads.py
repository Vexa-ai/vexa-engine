"""merge_note_type_heads

Revision ID: c9c068394dee
Revises: ab5e92a47f33, add_note_type_to_content
Create Date: 2024-12-25 17:53:57.467463

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c9c068394dee'
down_revision: Union[str, None] = ('ab5e92a47f33', 'add_note_type_to_content')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass