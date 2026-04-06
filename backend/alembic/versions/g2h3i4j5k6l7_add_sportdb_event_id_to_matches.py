"""add sportdb_event_id to matches

Revision ID: g2h3i4j5k6l7
Revises: f1a2b3c4d5e6
Branch Labels: None
Depends On: None

"""
from alembic import op
import sqlalchemy as sa

revision = 'g2h3i4j5k6l7'
down_revision = 'f1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'matches',
        sa.Column('sportdb_event_id', sa.String(100), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('matches', 'sportdb_event_id')
