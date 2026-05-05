"""Add sync papers arXiv request checkpoints.

Revision ID: 0012_arxiv_checkpoints
Revises: 0011_rename_sync_papers
Create Date: 2026-04-24 19:09:28
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0012_arxiv_checkpoints"
down_revision = "0011_rename_sync_papers"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return index_name in {item["name"] for item in _inspector().get_indexes(table_name)}


def upgrade() -> None:
    if _has_table("sync_papers_arxiv_request_checkpoints"):
        return

    op.create_table(
        "sync_papers_arxiv_request_checkpoints",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("attempt_series_key", sa.String(length=36), nullable=False),
        sa.Column("source_job_id", sa.String(length=36), nullable=True),
        sa.Column("surface", sa.String(length=64), nullable=False),
        sa.Column("request_key", sa.String(length=255), nullable=False),
        sa.Column("request_url", sa.String(length=1024), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("headers_json", sa.JSON(), nullable=False),
        sa.Column("body_path", sa.String(length=1024), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("raw_fetch_id", sa.String(length=36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["raw_fetch_id"], ["raw_fetches.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_sync_papers_arxiv_checkpoints_request",
        "sync_papers_arxiv_request_checkpoints",
        ["attempt_series_key", "surface", "request_key"],
        unique=True,
    )
    op.create_index(
        "ix_sync_papers_arxiv_checkpoints_source_job",
        "sync_papers_arxiv_request_checkpoints",
        ["source_job_id"],
        unique=False,
    )


def downgrade() -> None:
    if not _has_table("sync_papers_arxiv_request_checkpoints"):
        return

    if _has_index("sync_papers_arxiv_request_checkpoints", "ix_sync_papers_arxiv_checkpoints_source_job"):
        op.drop_index(
            "ix_sync_papers_arxiv_checkpoints_source_job",
            table_name="sync_papers_arxiv_request_checkpoints",
        )
    if _has_index("sync_papers_arxiv_request_checkpoints", "ix_sync_papers_arxiv_checkpoints_request"):
        op.drop_index(
            "ix_sync_papers_arxiv_checkpoints_request",
            table_name="sync_papers_arxiv_request_checkpoints",
        )
    op.drop_table("sync_papers_arxiv_request_checkpoints")
