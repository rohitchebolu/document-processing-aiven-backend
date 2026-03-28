"""Add durable job event history for live processing updates.

Revision ID: 0004_job_events
Revises: 0003_trim_llm
Create Date: 2026-03-28 17:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0004_job_events"
down_revision = "0003_trim_llm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("progress", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_job_events_id"), "job_events", ["id"], unique=False)
    op.create_index(op.f("ix_job_events_job_id"), "job_events", ["job_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_job_events_job_id"), table_name="job_events")
    op.drop_index(op.f("ix_job_events_id"), table_name="job_events")
    op.drop_table("job_events")
