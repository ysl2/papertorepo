"""Rename sync arxiv objects to sync papers.

Revision ID: 0011_rename_sync_papers
Revises: 0010_rename_repo_job_types
Create Date: 2026-04-24 17:09:49
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from alembic import op
import sqlalchemy as sa


revision = "0011_rename_sync_papers"
down_revision = "0010_rename_repo_job_types"
branch_labels = None
depends_on = None

OLD_TO_NEW_JOB_TYPES = {
    "sync_arxiv": "sync_papers",
    "sync_arxiv_batch": "sync_papers_batch",
}
NEW_TO_OLD_JOB_TYPES = {new: old for old, new in OLD_TO_NEW_JOB_TYPES.items()}


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return index_name in {item["name"] for item in _inspector().get_indexes(table_name)}


def _dedupe_key(job_type: str, scope_json: dict[str, object]) -> str:
    payload = json.dumps({"job_type": job_type, "scope": scope_json}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _coerce_scope_json(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return dict(loaded)
        raise ValueError("jobs.scope_json must decode to an object")
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    raise TypeError(f"Unsupported scope_json payload: {type(value).__name__}")


def _postgres_jobtype_labels() -> set[str]:
    result = op.get_bind().execute(
        sa.text(
            """
            SELECT enumlabel
            FROM pg_enum
            JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
            WHERE pg_type.typname = 'jobtype'
            """
        )
    )
    return {str(label) for label in result.scalars()}


def _prepare_postgresql_job_type_mapping(mapping: dict[str, str]) -> dict[str, str]:
    bind = op.get_bind()
    labels = _postgres_jobtype_labels()
    mappings_requiring_update: dict[str, str] = {}
    for old_value, new_value in mapping.items():
        if old_value in labels and new_value not in labels:
            bind.exec_driver_sql(f"ALTER TYPE jobtype RENAME VALUE '{old_value}' TO '{new_value}'")
            labels.remove(old_value)
            labels.add(new_value)
            continue
        if old_value in labels and new_value in labels:
            mappings_requiring_update[old_value] = new_value
            continue
        elif new_value not in labels:
            bind.exec_driver_sql(f"ALTER TYPE jobtype ADD VALUE IF NOT EXISTS '{new_value}'")
            labels.add(new_value)
    return mappings_requiring_update


def _update_job_type_values(mapping: dict[str, str]) -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        statement = sa.text(
            "UPDATE jobs SET job_type = CAST(:new_value AS jobtype) WHERE job_type = CAST(:old_value AS jobtype)"
        )
    else:
        statement = sa.text("UPDATE jobs SET job_type = :new_value WHERE job_type = :old_value")
    for old_value, new_value in mapping.items():
        bind.execute(
            statement,
            {"old_value": old_value, "new_value": new_value},
        )


def _refresh_dedupe_keys(valid_job_types: set[str]) -> None:
    if not _has_table("jobs"):
        return
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, job_type, scope_json FROM jobs")).mappings()
    for row in rows:
        job_type = str(row["job_type"])
        if job_type not in valid_job_types:
            continue
        scope_json = _coerce_scope_json(row["scope_json"])
        bind.execute(
            sa.text("UPDATE jobs SET dedupe_key = :dedupe_key WHERE id = :job_id"),
            {"job_id": row["id"], "dedupe_key": _dedupe_key(job_type, scope_json)},
        )


def _apply_job_type_mapping(mapping: dict[str, str]) -> None:
    if not _has_table("jobs"):
        return
    bind = op.get_bind()
    mappings_requiring_update = mapping
    if bind.dialect.name == "postgresql":
        mappings_requiring_update = _prepare_postgresql_job_type_mapping(mapping)
    _update_job_type_values(mappings_requiring_update)
    _refresh_dedupe_keys(set(mapping.values()))


def _drop_index_if_exists(table_name: str, index_name: str) -> None:
    if _has_index(table_name, index_name):
        op.drop_index(index_name, table_name=table_name)


def _create_index_if_missing(table_name: str, index_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns, unique=False)


def _rename_table_if_needed(old_name: str, new_name: str) -> None:
    if _has_table(old_name) and not _has_table(new_name):
        op.rename_table(old_name, new_name)


def _drop_arxiv_sync_windows() -> None:
    if not _has_table("arxiv_sync_windows"):
        return
    _drop_index_if_exists("arxiv_sync_windows", "ix_arxiv_sync_windows_completed")
    op.drop_table("arxiv_sync_windows")


def _rename_sync_tables_to_new() -> None:
    _rename_table_if_needed("arxiv_sync_days", "sync_papers_arxiv_days")
    if _has_table("sync_papers_arxiv_days"):
        _drop_index_if_exists("sync_papers_arxiv_days", "ix_arxiv_sync_days_completed")
        _create_index_if_missing(
            "sync_papers_arxiv_days",
            "ix_sync_papers_arxiv_days_completed",
            ["last_completed_at"],
        )

    _rename_table_if_needed("arxiv_archive_appearances", "sync_papers_arxiv_archive_appearances")
    if _has_table("sync_papers_arxiv_archive_appearances"):
        _drop_index_if_exists(
            "sync_papers_arxiv_archive_appearances",
            "ix_arxiv_archive_appearances_month_arxiv",
        )
        _drop_index_if_exists(
            "sync_papers_arxiv_archive_appearances",
            "ix_arxiv_archive_appearances_category_month",
        )
        _create_index_if_missing(
            "sync_papers_arxiv_archive_appearances",
            "ix_sync_papers_arxiv_archive_appearances_month_arxiv",
            ["archive_month", "arxiv_id"],
        )
        _create_index_if_missing(
            "sync_papers_arxiv_archive_appearances",
            "ix_sync_papers_arxiv_archive_appearances_category_month",
            ["category", "archive_month"],
        )


def _rename_sync_tables_to_old() -> None:
    _rename_table_if_needed("sync_papers_arxiv_days", "arxiv_sync_days")
    if _has_table("arxiv_sync_days"):
        _drop_index_if_exists("arxiv_sync_days", "ix_sync_papers_arxiv_days_completed")
        _create_index_if_missing("arxiv_sync_days", "ix_arxiv_sync_days_completed", ["last_completed_at"])

    _rename_table_if_needed("sync_papers_arxiv_archive_appearances", "arxiv_archive_appearances")
    if _has_table("arxiv_archive_appearances"):
        _drop_index_if_exists(
            "arxiv_archive_appearances",
            "ix_sync_papers_arxiv_archive_appearances_month_arxiv",
        )
        _drop_index_if_exists(
            "arxiv_archive_appearances",
            "ix_sync_papers_arxiv_archive_appearances_category_month",
        )
        _create_index_if_missing(
            "arxiv_archive_appearances",
            "ix_arxiv_archive_appearances_month_arxiv",
            ["archive_month", "arxiv_id"],
        )
        _create_index_if_missing(
            "arxiv_archive_appearances",
            "ix_arxiv_archive_appearances_category_month",
            ["category", "archive_month"],
        )


def _restore_arxiv_sync_windows() -> None:
    if _has_table("arxiv_sync_windows"):
        return
    op.create_table(
        "arxiv_sync_windows",
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("last_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("category", "start_date", "end_date"),
    )
    op.create_index("ix_arxiv_sync_windows_completed", "arxiv_sync_windows", ["last_completed_at"], unique=False)


def upgrade() -> None:
    _apply_job_type_mapping(OLD_TO_NEW_JOB_TYPES)
    _rename_sync_tables_to_new()
    _drop_arxiv_sync_windows()


def downgrade() -> None:
    _apply_job_type_mapping(NEW_TO_OLD_JOB_TYPES)
    _rename_sync_tables_to_old()
    _restore_arxiv_sync_windows()
