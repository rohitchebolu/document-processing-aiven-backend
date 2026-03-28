"""Create baseline tables for the document processing flow.

Revision ID: 0001_doc_proc
Revises:
Create Date: 2026-03-28 12:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_doc_proc"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "extraction_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("original_filename", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("channel", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("prompt_name", sa.String(length=128), nullable=True),
        sa.Column("prompt_template", sa.Text(), nullable=True),
        sa.Column("extraction_result", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_extraction_jobs_id"), "extraction_jobs", ["id"], unique=False)
    op.create_index(
        op.f("ix_extraction_jobs_job_id"),
        "extraction_jobs",
        ["job_id"],
        unique=True,
    )

    op.create_table(
        "llm_usage_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("call_type", sa.Text(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("response_time_ms", sa.Integer(), nullable=False),
        sa.Column("extraction_job_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["extraction_job_id"], ["extraction_jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_llm_usage_logs_id"), "llm_usage_logs", ["id"], unique=False)
    op.create_index(
        "ix_llm_usage_logs_job_id",
        "llm_usage_logs",
        ["extraction_job_id"],
        unique=False,
    )

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

    op.drop_index("ix_llm_usage_logs_job_id", table_name="llm_usage_logs")
    op.drop_index(op.f("ix_llm_usage_logs_id"), table_name="llm_usage_logs")
    op.drop_table("llm_usage_logs")

    op.drop_index(op.f("ix_extraction_jobs_job_id"), table_name="extraction_jobs")
    op.drop_index(op.f("ix_extraction_jobs_id"), table_name="extraction_jobs")
    op.drop_table("extraction_jobs")
