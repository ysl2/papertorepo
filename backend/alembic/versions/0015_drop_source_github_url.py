"""Drop source GitHub URL metadata.

Revision ID: 0015_drop_source_github_url
Revises: 0014_github_url_metadata
Create Date: 2026-04-26 02:15:24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0015_drop_source_github_url"
down_revision = "0014_github_url_metadata"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return column_name in {item["name"] for item in _inspector().get_columns(table_name)}


def upgrade() -> None:
    if not _has_column("github_repos", "source_github_url"):
        return
    with op.batch_alter_table("github_repos") as batch_op:
        batch_op.drop_column("source_github_url")


def downgrade() -> None:
    if not _has_table("github_repos") or _has_column("github_repos", "source_github_url"):
        return
    with op.batch_alter_table("github_repos") as batch_op:
        batch_op.add_column(sa.Column("source_github_url", sa.String(length=255), nullable=True))
