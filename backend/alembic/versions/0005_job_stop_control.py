"""Add job stop control fields and cancelled status.

Revision ID: 0005_job_stop_control
Revises: 0004_arxiv_archive_appearances
Create Date: 2026-04-21 18:10:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_job_stop_control"
down_revision = "0004_arxiv_archive_appearances"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'cancelled'")

    if not _has_table("jobs"):
        return

    if not _has_column("jobs", "stop_requested_at"):
        op.add_column("jobs", sa.Column("stop_requested_at", sa.DateTime(timezone=True), nullable=True))
    if not _has_column("jobs", "stop_reason"):
        op.add_column("jobs", sa.Column("stop_reason", sa.String(length=64), nullable=True))


def downgrade() -> None:
    if not _has_table("jobs"):
        return

    if _has_column("jobs", "stop_reason"):
        op.drop_column("jobs", "stop_reason")
    if _has_column("jobs", "stop_requested_at"):
        op.drop_column("jobs", "stop_requested_at")
