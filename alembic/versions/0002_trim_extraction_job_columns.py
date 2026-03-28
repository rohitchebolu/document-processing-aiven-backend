"""Drop unused extraction job metadata columns.

Revision ID: 0002_trim_jobs
Revises: 0001_doc_proc
Create Date: 2026-03-28 13:30:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_trim_jobs"
down_revision = "0001_doc_proc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
