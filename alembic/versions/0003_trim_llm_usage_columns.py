"""Drop unused organization column from llm usage logs.

Revision ID: 0003_trim_llm
Revises: 0002_trim_jobs
Create Date: 2026-03-28 13:45:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_trim_llm"
down_revision = "0002_trim_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
