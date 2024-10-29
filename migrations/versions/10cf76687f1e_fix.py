"""fix

Revision ID: 10cf76687f1e
Revises: d005f42e37d2
Create Date: 2024-10-29 19:37:33.182398

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '10cf76687f1e'
down_revision: Union[str, None] = 'd005f42e37d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('user_meetings',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('meeting_id', sa.UUID(), nullable=True),
    sa.Column('user_id', sa.UUID(), nullable=True),
    sa.Column('access_level', sa.String(length=20), server_default='search', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_by', sa.UUID(), nullable=True),
    sa.Column('is_owner', sa.Boolean(), nullable=False),
    sa.CheckConstraint("access_level IN ('removed', 'search', 'transcript', 'owner')", name='valid_access_level'),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['meeting_id'], ['meetings.meeting_id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('meeting_id', 'user_id', name='uq_user_meeting')
    )
    op.drop_table('meeting_shares')
    op.drop_constraint('meetings_owner_id_fkey', 'meetings', type_='foreignkey')
    op.drop_column('meetings', 'owner_id')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('meetings', sa.Column('owner_id', sa.UUID(), autoincrement=False, nullable=False))
    op.create_foreign_key('meetings_owner_id_fkey', 'meetings', 'users', ['owner_id'], ['id'])
    op.create_table('meeting_shares',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('meeting_id', sa.UUID(), autoincrement=False, nullable=True),
    sa.Column('user_id', sa.UUID(), autoincrement=False, nullable=True),
    sa.Column('access_level', sa.VARCHAR(length=20), server_default=sa.text("'search'::character varying"), autoincrement=False, nullable=False),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.UUID(), autoincrement=False, nullable=True),
    sa.CheckConstraint("access_level::text = ANY (ARRAY['removed'::character varying, 'search'::character varying, 'transcript'::character varying]::text[])", name='valid_access_level'),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], name='meeting_shares_created_by_fkey'),
    sa.ForeignKeyConstraint(['meeting_id'], ['meetings.meeting_id'], name='meeting_shares_meeting_id_fkey'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='meeting_shares_user_id_fkey'),
    sa.PrimaryKeyConstraint('id', name='meeting_shares_pkey'),
    sa.UniqueConstraint('meeting_id', 'user_id', name='uq_meeting_user_share')
    )
    op.drop_table('user_meetings')
    # ### end Alembic commands ###