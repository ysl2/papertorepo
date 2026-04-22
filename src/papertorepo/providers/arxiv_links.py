from __future__ import annotations

import asyncio
import re

import aiohttp

from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.github import extract_github_repo_urls


ARXIV_ABS_GITHUB_PATTERN = re.compile(r'https?://(?:www\.)?github\.com/[^\s\"<>]+', re.IGNORECASE)


class ArxivLinksClient:
    def __init__(self, session: aiohttp.ClientSession, *, min_interval: float = 0.5, max_concurrent: int = 2):
        self.session = session
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(min_interval)

    async def fetch_abs_html(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://arxiv.org/abs/{arxiv_id}",
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv abs page",
        )


def extract_github_urls_from_comment(comment: str | None) -> tuple[str, ...]:
    return extract_github_repo_urls(comment or "")


def extract_github_urls_from_abs_html(html: str | None) -> tuple[str, ...]:
    if not html or not isinstance(html, str):
        return ()
    return extract_github_repo_urls(" ".join(ARXIV_ABS_GITHUB_PATTERN.findall(html)))
