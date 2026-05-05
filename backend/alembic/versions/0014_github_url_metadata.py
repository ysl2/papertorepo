"""Converge GitHub URL identity and metadata.

Revision ID: 0014_github_url_metadata
Revises: 0013_job_item_resume_progress
Create Date: 2026-04-26 01:29:11
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from alembic import op
import sqlalchemy as sa


revision = "0014_github_url_metadata"
down_revision = "0013_job_item_resume_progress"
branch_labels = None
depends_on = None


GITHUB_HOSTS = {"github.com", "www.github.com"}
RESERVED_OWNER_SEGMENTS = {
    "about",
    "account",
    "apps",
    "collections",
    "contact",
    "enterprise",
    "events",
    "explore",
    "features",
    "issues",
    "join",
    "login",
    "marketplace",
    "new",
    "notifications",
    "orgs",
    "pricing",
    "pulls",
    "readme",
    "search",
    "security",
    "settings",
    "site",
    "sponsors",
    "team",
    "teams",
    "topics",
    "trending",
    "users",
}


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _columns(table_name: str) -> set[str]:
    if not _has_table(table_name):
        return set()
    return {item["name"] for item in _inspector().get_columns(table_name)}


def _has_column(table_name: str, column_name: str) -> bool:
    return column_name in _columns(table_name)


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return index_name in {item["name"] for item in _inspector().get_indexes(table_name)}


def _normalize_github_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    host = (parsed.hostname or parsed.netloc or "").lower()
    if parsed.scheme not in {"http", "https"} or host not in GITHUB_HOSTS:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None
    owner = parts[0].strip().lower()
    repo = re.sub(r"\.git$", "", parts[1].strip(), flags=re.IGNORECASE).lower()
    if not owner or not repo or owner in RESERVED_OWNER_SEGMENTS:
        return None
    return f"https://github.com/{owner}/{repo}"


def _name_with_owner(github_url: str, row: dict[str, Any]) -> str:
    owner = str(row.get("owner") or "").strip()
    repo = str(row.get("repo") or "").strip()
    if owner and repo:
        return f"{owner}/{repo}"
    parsed = urlparse(github_url)
    parts = [part for part in parsed.path.split("/") if part]
    return f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else github_url.removeprefix("https://github.com/")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _first_topic(value: Any) -> str | None:
    for item in _json_list(value):
        topic = str(item).strip()
        if topic:
            return topic
    return None


def _completion_score(row: dict[str, Any]) -> tuple[int, str]:
    useful_keys = [
        "github_id",
        "stars",
        "created_at",
        "description",
        "homepage",
        "topics_json",
        "license",
        "pushed_at",
        "checked_at",
    ]
    score = 0
    for key in useful_keys:
        value = row.get(key)
        if value not in (None, "", [], {}):
            score += 1
    checked_at = row.get("checked_at")
    if isinstance(checked_at, datetime):
        checked_key = checked_at.isoformat()
    else:
        checked_key = str(checked_at or "")
    return score, checked_key


def _dedupe_github_repo_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    winners: dict[str, dict[str, Any]] = {}
    for row in rows:
        github_url = _normalize_github_url(row.get("normalized_github_url"))
        if github_url is None:
            continue
        current = winners.get(github_url)
        if current is None or _completion_score(row) > _completion_score(current):
            winners[github_url] = row

    records: list[dict[str, Any]] = []
    for github_url in sorted(winners):
        row = winners[github_url]
        records.append(
            {
                "github_url": github_url,
                "github_id": row.get("github_id"),
                "node_id": None,
                "name_with_owner": _name_with_owner(github_url, row),
                "description": row.get("description"),
                "homepage": row.get("homepage"),
                "stargazers_count": row.get("stars"),
                "forks_count": None,
                "size_kb": None,
                "primary_language": None,
                "topic": _first_topic(row.get("topics_json")),
                "license_spdx_id": row.get("license"),
                "license_name": None,
                "default_branch": None,
                "is_private": None,
                "visibility": None,
                "is_fork": None,
                "is_archived": row.get("archived"),
                "is_template": None,
                "is_disabled": None,
                "has_issues": None,
                "has_projects": None,
                "has_wiki": None,
                "has_discussions": None,
                "allow_forking": None,
                "web_commit_signoff_required": None,
                "parent_github_url": None,
                "source_github_url": None,
                "created_at": row.get("created_at"),
                "updated_at": None,
                "pushed_at": row.get("pushed_at"),
            }
        )
    return records


def _create_github_repos_table(table_name: str) -> None:
    op.create_table(
        table_name,
        sa.Column("github_url", sa.String(length=255), nullable=False),
        sa.Column("github_id", sa.Integer(), nullable=True),
        sa.Column("node_id", sa.String(length=255), nullable=True),
        sa.Column("name_with_owner", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("homepage", sa.String(length=1024), nullable=True),
        sa.Column("stargazers_count", sa.Integer(), nullable=True),
        sa.Column("forks_count", sa.Integer(), nullable=True),
        sa.Column("size_kb", sa.Integer(), nullable=True),
        sa.Column("primary_language", sa.String(length=255), nullable=True),
        sa.Column("topic", sa.String(length=255), nullable=True),
        sa.Column("license_spdx_id", sa.String(length=255), nullable=True),
        sa.Column("license_name", sa.String(length=255), nullable=True),
        sa.Column("default_branch", sa.String(length=255), nullable=True),
        sa.Column("is_private", sa.Boolean(), nullable=True),
        sa.Column("visibility", sa.String(length=64), nullable=True),
        sa.Column("is_fork", sa.Boolean(), nullable=True),
        sa.Column("is_archived", sa.Boolean(), nullable=True),
        sa.Column("is_template", sa.Boolean(), nullable=True),
        sa.Column("is_disabled", sa.Boolean(), nullable=True),
        sa.Column("has_issues", sa.Boolean(), nullable=True),
        sa.Column("has_projects", sa.Boolean(), nullable=True),
        sa.Column("has_wiki", sa.Boolean(), nullable=True),
        sa.Column("has_discussions", sa.Boolean(), nullable=True),
        sa.Column("allow_forking", sa.Boolean(), nullable=True),
        sa.Column("web_commit_signoff_required", sa.Boolean(), nullable=True),
        sa.Column("parent_github_url", sa.String(length=255), nullable=True),
        sa.Column("source_github_url", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.String(length=64), nullable=True),
        sa.Column("pushed_at", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("github_url"),
    )


def _rebuild_github_repos() -> None:
    if not _has_table("github_repos") or not _has_column("github_repos", "normalized_github_url"):
        return
    connection = op.get_bind()
    rows = [dict(row) for row in connection.execute(sa.text("SELECT * FROM github_repos")).mappings()]
    records = _dedupe_github_repo_rows(rows)
    new_name = "github_repos_0014_new"
    if _has_table(new_name):
        op.drop_table(new_name)
    _create_github_repos_table(new_name)
    if records:
        connection.execute(
            sa.text(
                """
                INSERT INTO github_repos_0014_new (
                    github_url, github_id, node_id, name_with_owner, description, homepage,
                    stargazers_count, forks_count, size_kb, primary_language, topic,
                    license_spdx_id, license_name, default_branch, is_private, visibility,
                    is_fork, is_archived, is_template, is_disabled, has_issues, has_projects,
                    has_wiki, has_discussions, allow_forking, web_commit_signoff_required,
                    parent_github_url, source_github_url, created_at, updated_at, pushed_at
                )
                VALUES (
                    :github_url, :github_id, :node_id, :name_with_owner, :description, :homepage,
                    :stargazers_count, :forks_count, :size_kb, :primary_language, :topic,
                    :license_spdx_id, :license_name, :default_branch, :is_private, :visibility,
                    :is_fork, :is_archived, :is_template, :is_disabled, :has_issues, :has_projects,
                    :has_wiki, :has_discussions, :allow_forking, :web_commit_signoff_required,
                    :parent_github_url, :source_github_url, :created_at, :updated_at, :pushed_at
                )
                """
            ),
            records,
        )
    op.drop_table("github_repos")
    op.rename_table(new_name, "github_repos")
    op.create_index("ix_github_repos_github_id", "github_repos", ["github_id"], unique=False)
    op.create_index("ix_github_repos_node_id", "github_repos", ["node_id"], unique=False)


def _rename_column_if_needed(table_name: str, old_name: str, new_name: str, column_type: sa.types.TypeEngine) -> None:
    if not _has_table(table_name) or not _has_column(table_name, old_name) or _has_column(table_name, new_name):
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.alter_column(old_name, new_column_name=new_name, existing_type=column_type)


def _canonicalize_paper_repo_state() -> None:
    if not _has_table("paper_repo_state"):
        return
    _rename_column_if_needed("paper_repo_state", "primary_repo_url", "primary_github_url", sa.String(length=255))
    _rename_column_if_needed("paper_repo_state", "repo_urls_json", "github_urls_json", sa.JSON())
    if not {"arxiv_id", "primary_github_url", "github_urls_json"}.issubset(_columns("paper_repo_state")):
        return

    connection = op.get_bind()
    table = sa.table(
        "paper_repo_state",
        sa.column("arxiv_id", sa.String()),
        sa.column("primary_github_url", sa.String()),
        sa.column("github_urls_json", sa.JSON()),
    )
    rows = connection.execute(sa.select(table.c.arxiv_id, table.c.primary_github_url, table.c.github_urls_json)).all()
    for arxiv_id, primary_url, urls_json in rows:
        canonical_primary = _normalize_github_url(primary_url)
        canonical_urls: list[str] = []
        seen: set[str] = set()
        for raw_url in _json_list(urls_json):
            canonical = _normalize_github_url(raw_url)
            if canonical is None or canonical in seen:
                continue
            seen.add(canonical)
            canonical_urls.append(canonical)
        if canonical_primary is not None and canonical_primary not in seen:
            canonical_urls.insert(0, canonical_primary)
        elif canonical_primary is None and canonical_urls:
            canonical_primary = canonical_urls[0]
        connection.execute(
            table.update()
            .where(table.c.arxiv_id == arxiv_id)
            .values(primary_github_url=canonical_primary, github_urls_json=canonical_urls)
        )


def _canonicalize_repo_observations() -> None:
    if not _has_table("repo_observations"):
        return
    _rename_column_if_needed("repo_observations", "observed_repo_url", "observed_github_url", sa.String(length=1024))
    _rename_column_if_needed("repo_observations", "normalized_repo_url", "github_url", sa.String(length=255))
    columns = _columns("repo_observations")
    if not {"id", "observed_github_url", "github_url"}.issubset(columns):
        return

    connection = op.get_bind()
    table = sa.table(
        "repo_observations",
        sa.column("id", sa.Integer()),
        sa.column("observed_github_url", sa.String()),
        sa.column("github_url", sa.String()),
    )
    rows = connection.execute(sa.select(table.c.id, table.c.observed_github_url, table.c.github_url)).all()
    for row_id, observed_url, github_url in rows:
        canonical_observed = _normalize_github_url(observed_url)
        canonical_github = _normalize_github_url(github_url) or canonical_observed
        connection.execute(
            table.update()
            .where(table.c.id == row_id)
            .values(observed_github_url=canonical_observed, github_url=canonical_github)
        )


def _canonicalize_resume_repo_keys() -> None:
    if not _has_table("job_item_resume_progress"):
        return
    connection = op.get_bind()
    if _has_index("job_item_resume_progress", "ix_job_item_resume_progress_item"):
        op.drop_index("ix_job_item_resume_progress_item", table_name="job_item_resume_progress")

    table = sa.table(
        "job_item_resume_progress",
        sa.column("id", sa.String()),
        sa.column("attempt_series_key", sa.String()),
        sa.column("job_type", sa.String()),
        sa.column("item_kind", sa.String()),
        sa.column("item_key", sa.String()),
    )
    rows = connection.execute(
        sa.select(
            table.c.id,
            table.c.attempt_series_key,
            table.c.job_type,
            table.c.item_kind,
            table.c.item_key,
        )
    ).all()
    seen: set[tuple[str, str, str, str]] = set()
    delete_ids: list[str] = []
    for row_id, attempt_series_key, job_type, item_kind, item_key in rows:
        canonical_key = _normalize_github_url(item_key) if item_kind == "repo" else item_key
        if canonical_key is None:
            canonical_key = item_key
        dedupe_key = (attempt_series_key, job_type, item_kind, canonical_key)
        if dedupe_key in seen:
            delete_ids.append(row_id)
            continue
        seen.add(dedupe_key)
        if canonical_key != item_key:
            connection.execute(table.update().where(table.c.id == row_id).values(item_key=canonical_key))
    if delete_ids:
        connection.execute(table.delete().where(table.c.id.in_(delete_ids)))

    op.create_index(
        "ix_job_item_resume_progress_item",
        "job_item_resume_progress",
        ["attempt_series_key", "job_type", "item_kind", "item_key"],
        unique=True,
    )


def upgrade() -> None:
    _rebuild_github_repos()
    _canonicalize_paper_repo_state()
    _canonicalize_repo_observations()
    _canonicalize_resume_repo_keys()


def downgrade() -> None:
    pass
