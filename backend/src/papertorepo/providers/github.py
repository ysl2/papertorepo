from __future__ import annotations

import asyncio

import aiohttp

from papertorepo.core.config import get_settings
from papertorepo.core.records import GitHubRepoMetadata
from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.github import extract_owner_repo, normalize_github_url


class GitHubClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        github_token: str = "",
        min_interval: float | None = None,
        max_concurrent: int | None = None,
    ):
        settings = get_settings()
        effective_min_interval = settings.refresh_metadata_github_min_interval if min_interval is None else min_interval
        effective_max_concurrent = (
            settings.refresh_metadata_github_rest_fallback_max_concurrent if max_concurrent is None else max_concurrent
        )
        self.session = session
        self.github_token = github_token
        self.semaphore = asyncio.Semaphore(max(1, effective_max_concurrent))
        self.rate_limiter = RateLimiter(
            effective_min_interval
            if github_token.strip()
            else max(effective_min_interval, settings.refresh_metadata_github_anonymous_min_interval)
        )

    async def fetch_repo_metadata(self, github_url: str) -> tuple[GitHubRepoMetadata | None, str | None]:
        owner_repo = extract_owner_repo(github_url)
        if owner_repo is None:
            return None, "GitHub URL is not a valid GitHub repository"
        owner, repo = owner_repo
        status, body, _headers, error = await request_text(
            self.session,
            f"https://api.github.com/repos/{owner}/{repo}",
            headers=self._build_headers(),
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="GitHub API",
            allowed_statuses={404},
        )
        if error:
            return None, error
        if status == 404:
            return None, "Repository not found"
        if body is None:
            return None, "GitHub API returned empty body"
        try:
            import json

            payload = json.loads(body)
        except json.JSONDecodeError:
            return None, "GitHub API returned invalid JSON"
        return (
            GitHubRepoMetadata(
                github_url=normalize_github_url(github_url) or github_url,
                name_with_owner=payload.get("full_name") or f"{owner}/{repo}",
                stargazers_count=payload.get("stargazers_count"),
                created_at=payload.get("created_at"),
                description=payload.get("description") if payload.get("description") is not None else "",
                homepage=payload.get("homepage"),
                topic=(payload.get("topics") or [None])[0] if isinstance(payload.get("topics"), list) else None,
                license_spdx_id=((payload.get("license") or {}).get("spdx_id") if isinstance(payload.get("license"), dict) else None),
                license_name=((payload.get("license") or {}).get("name") if isinstance(payload.get("license"), dict) else None),
            ),
            None,
        )

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "papertorepo",
        }
        if self.github_token:
            headers["Authorization"] = f"Bearer {self.github_token}"
        return headers
