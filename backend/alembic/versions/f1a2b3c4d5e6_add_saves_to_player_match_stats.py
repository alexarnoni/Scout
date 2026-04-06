"""add saves to player_match_stats

Revision ID: f1a2b3c4d5e6
Revises: 209171472d94
Branch Labels: None
Depends On: None

"""
from alembic import op
import sqlalchemy as sa

revision = 'f1a2b3c4d5e6'
down_revision = 'e1f2a3b4c5d6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'player_match_stats',
        sa.Column('saves', sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('player_match_stats', 'saves')
