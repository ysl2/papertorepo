from __future__ import annotations

import asyncio

import aiohttp

from papertorepo.core.records import GitHubRepoMetadata
from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.github import extract_owner_repo, normalize_github_url


class GitHubClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        github_token: str = "",
        min_interval: float = 0.5,
        max_concurrent: int = 2,
    ):
        self.session = session
        self.github_token = github_token
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(min_interval if github_token.strip() else max(min_interval, 60.0))

    async def fetch_repo_metadata(self, normalized_github_url: str) -> tuple[GitHubRepoMetadata | None, str | None]:
        owner_repo = extract_owner_repo(normalized_github_url)
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
                normalized_github_url=normalize_github_url(normalized_github_url) or normalized_github_url,
                owner=owner,
                repo=repo,
                stars=payload.get("stargazers_count"),
                created_at=payload.get("created_at"),
                description=payload.get("description") if payload.get("description") is not None else "",
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
