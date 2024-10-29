"""add_default_access_table

Revision ID: 20241029_default_access
Revises: 0334bafca1e2
Create Date: 2024-10-29 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20241029_default_access'
down_revision = '0334bafca1e2'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Create default_access table
    op.create_table(
        'default_access',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('owner_user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('granted_user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('access_level', sa.String(), nullable=False, server_default='search'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['owner_user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['granted_user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('owner_user_id', 'granted_user_id', name='uq_default_access_owner_granted')
    )

    # Add check constraint
    op.execute("""
        ALTER TABLE default_access 
        ADD CONSTRAINT valid_default_access_level 
        CHECK (access_level IN ('removed', 'search', 'transcript', 'owner'));
    """)

    # Update existing meeting_shares constraint
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_access_level') THEN
                ALTER TABLE meeting_shares 
                DROP CONSTRAINT valid_access_level;
            END IF;
        END $$;
        
        ALTER TABLE meeting_shares 
        ADD CONSTRAINT valid_access_level 
        CHECK (access_level IN ('removed', 'search', 'transcript', 'owner'));
    """)

    # Add indexes
    op.create_index('idx_default_access_owner', 'default_access', ['owner_user_id'])
    op.create_index('idx_default_access_granted', 'default_access', ['granted_user_id'])

def downgrade() -> None:
    # Revert meeting_shares constraint
    op.execute("""
        ALTER TABLE meeting_shares 
        DROP CONSTRAINT IF EXISTS valid_access_level;
        
        ALTER TABLE meeting_shares 
        ADD CONSTRAINT valid_access_level 
        CHECK (access_level IN ('removed', 'search', 'transcript'));
    """)

    # Drop default_access table and its dependencies
    op.drop_index('idx_default_access_granted')
    op.drop_index('idx_default_access_owner')
    op.drop_table('default_access')