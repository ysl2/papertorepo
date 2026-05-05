from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Paper:
    arxiv_id: str
    entry_id: str | None
    abs_url: str
    title: str
    abstract: str
    published_at: datetime | None
    updated_at: datetime | None
    authors: tuple[str, ...]
    author_details: tuple[dict[str, Any], ...]
    categories: tuple[str, ...]
    category_details: tuple[dict[str, Any], ...]
    links: tuple[dict[str, Any], ...]
    comment: str | None
    journal_ref: str | None
    doi: str | None
    primary_category: str | None
    primary_category_scheme: str | None


@dataclass(frozen=True)
class RawCacheEntry:
    id: int
    provider: str
    surface: str
    request_key: str
    request_url: str
    content_type: str | None
    status_code: int
    body_path: Path
    content_hash: str
    fetched_at: str
    etag: str | None
    last_modified: str | None


@dataclass(frozen=True)
class PaperSourceSnapshot:
    id: int
    arxiv_id: str
    provider: str
    surface: str
    raw_cache_id: int | None
    data_json: str
    fetched_at: str


@dataclass(frozen=True)
class RepoObservation:
    id: int
    arxiv_id: str
    provider: str
    surface: str
    status: str
    observed_github_url: str | None
    github_url: str | None
    evidence_text: str | None
    raw_cache_id: int | None
    extractor_version: str
    error_message: str | None
    observed_at: str


@dataclass(frozen=True)
class PaperRepoLink:
    id: int
    arxiv_id: str
    github_url: str
    status: str
    providers: tuple[str, ...]
    surfaces: tuple[str, ...]
    provider_count: int
    surface_count: int
    is_primary: bool
    resolved_at: str


@dataclass(frozen=True)
class PaperLinkSyncState:
    arxiv_id: str
    status: str
    checked_at: str


@dataclass(frozen=True)
class PaperSyncLease:
    arxiv_id: str
    owner_id: str
    lease_token: str
    acquired_at: str
    heartbeat_at: str
    lease_expires_at: str


@dataclass(frozen=True)
class ResourceLease:
    resource_key: str
    owner_id: str
    lease_token: str
    acquired_at: str
    heartbeat_at: str
    lease_expires_at: str


@dataclass(frozen=True)
class GitHubRepoMetadata:
    github_url: str
    name_with_owner: str | None
    stargazers_count: int | None
    created_at: str | None
    description: str | None
    homepage: str | None = None
    topic: str | None = None
    license_spdx_id: str | None = None
    license_name: str | None = None
