"""Rename repo job types to find_repos and refresh_metadata.

Revision ID: 0010_rename_repo_job_types
Revises: 0009_arxiv_daily_feed_fields
Create Date: 2026-04-23 14:10:00
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from alembic import op
import sqlalchemy as sa


revision = "0010_rename_repo_job_types"
down_revision = "0009_arxiv_daily_feed_fields"
branch_labels = None
depends_on = None

OLD_TO_NEW_JOB_TYPES = {
    "sync_links": "find_repos",
    "sync_links_batch": "find_repos_batch",
    "enrich": "refresh_metadata",
    "enrich_batch": "refresh_metadata_batch",
}
NEW_TO_OLD_JOB_TYPES = {new: old for old, new in OLD_TO_NEW_JOB_TYPES.items()}


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


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
    bind = op.get_bind()
    result = bind.execute(
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


def _rename_postgresql_enum_values(mapping: dict[str, str]) -> None:
    bind = op.get_bind()
    labels = _postgres_jobtype_labels()
    for old_value, new_value in mapping.items():
        if old_value not in labels or new_value in labels:
            continue
        bind.exec_driver_sql(f"ALTER TYPE jobtype RENAME VALUE '{old_value}' TO '{new_value}'")
        labels.remove(old_value)
        labels.add(new_value)


def _update_job_type_values(mapping: dict[str, str]) -> None:
    bind = op.get_bind()
    for old_value, new_value in mapping.items():
        bind.execute(
            sa.text("UPDATE jobs SET job_type = :new_value WHERE job_type = :old_value"),
            {"old_value": old_value, "new_value": new_value},
        )


def _refresh_dedupe_keys(valid_job_types: set[str]) -> None:
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
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        _rename_postgresql_enum_values(mapping)
    else:
        _update_job_type_values(mapping)
    _refresh_dedupe_keys(set(mapping.values()))


def upgrade() -> None:
    if not _has_table("jobs"):
        return
    _apply_job_type_mapping(OLD_TO_NEW_JOB_TYPES)


def downgrade() -> None:
    if not _has_table("jobs"):
        return
    _apply_job_type_mapping(NEW_TO_OLD_JOB_TYPES)
