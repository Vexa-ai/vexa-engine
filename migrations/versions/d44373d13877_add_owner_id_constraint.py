"""add_owner_id_constraint

Revision ID: d44373d13877
Revises: 3b01680a9754
Create Date: 2024-10-27 21:54:58.795857

"""
from typing import Sequence, Union


from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'd44373d13877'
down_revision: Union[str, None] = '3b01680a9754'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade() -> None:
    # First, set a default owner for existing meetings
    # You'll need to choose a user_id from your users table
    op.execute("""
        UPDATE meetings
        SET owner_id = (SELECT id FROM users LIMIT 1)
        WHERE owner_id IS NULL
    """)
    
    # Then add the not null constraint
    op.alter_column('meetings', 'owner_id',
        existing_type=postgresql.UUID(),
        nullable=False,
        existing_server_default=None,
        existing_comment=None
    )

def downgrade() -> None:
    # Remove the not null constraint
    op.alter_column('meetings', 'owner_id',
        existing_type=postgresql.UUID(),
        nullable=True,
        existing_server_default=None,
        existing_comment=None
    )