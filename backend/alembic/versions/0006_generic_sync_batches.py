"""Add generic sync batch job types.

Revision ID: 0006_generic_sync_batches
Revises: 0005_job_stop_control
Create Date: 2026-04-22 20:15:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0006_generic_sync_batches"
down_revision = "0005_job_stop_control"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_table("jobs"):
        return

    if bind.dialect.name == "postgresql":
        bind.exec_driver_sql("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'find_repos_batch'")
        bind.exec_driver_sql("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'refresh_metadata_batch'")


def downgrade() -> None:
    return
