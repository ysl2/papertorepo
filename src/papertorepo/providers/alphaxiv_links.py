from __future__ import annotations

import asyncio
import html
import json
import re

import aiohttp

from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.github import normalize_github_url


GITHUB_URL_PATTERN = re.compile(r"https?://(?:www\.)?github\.com/[\w.-]+/[\w.-]+(?:\.git)?/?[),.;:!?]*", re.IGNORECASE)


class AlphaXivLinksClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        alphaxiv_token: str = "",
        min_interval: float = 0.2,
        max_concurrent: int = 2,
    ):
        self.session = session
        self.alphaxiv_token = alphaxiv_token
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(min_interval)

    async def fetch_paper_payload(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://api.alphaxiv.org/papers/v3/{arxiv_id}",
            headers=self._build_headers("application/json"),
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="AlphaXiv paper API",
            allowed_statuses={404},
        )

    async def fetch_paper_html(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://www.alphaxiv.org/abs/{arxiv_id}",
            headers=self._build_headers("text/html,application/xhtml+xml"),
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="AlphaXiv paper page",
            allowed_statuses={404},
        )

    def _build_headers(self, accept: str) -> dict[str, str]:
        headers = {
            "Accept": accept,
            "User-Agent": "papertorepo",
        }
        if self.alphaxiv_token:
            headers["Authorization"] = f"Bearer {self.alphaxiv_token}"
        return headers


def extract_github_url_from_alphaxiv_payload(payload_text: str | None) -> tuple[str, ...]:
    if not payload_text or not isinstance(payload_text, str):
        return ()
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ()
    if not isinstance(payload, dict):
        return ()

    paper = payload.get("paper", {}) if isinstance(payload.get("paper"), dict) else {}
    candidates = [
        paper.get("implementation"),
        paper.get("marimo_implementation"),
        paper.get("paper_group", {}).get("resources") if isinstance(paper.get("paper_group"), dict) else None,
        paper.get("resources"),
        payload,
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        for url in _iter_github_urls_from_json(candidate):
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return tuple(urls)


def extract_github_url_from_alphaxiv_html(html_text: str | None) -> tuple[str, ...]:
    if not html_text or not isinstance(html_text, str):
        return ()

    candidates = [html_text]
    decoded_html = html.unescape(html_text)
    if decoded_html != html_text:
        candidates.insert(0, decoded_html)

    urls: list[str] = []
    seen: set[str] = set()
    patterns = (
        r'resources:\$R\[\d+\]=\{github:\$R\[\d+\]=\{url:"(https://github\.com/[^"]+)"',
        r'resources:\{github:\{url:"(https://github\.com/[^"]+)"',
        r'"resources"\s*:\s*\{\s*"github"\s*:\s*\{\s*"url"\s*:\s*"(https://github\.com/[^"]+)"',
        r'\bimplementation:"(https://github\.com/[^"]+)"',
        r'"implementation"\s*:\s*"(https://github\.com/[^"]+)"',
        r'\bmarimo_implementation:"(https://github\.com/[^"]+)"',
        r'"marimo_implementation"\s*:\s*"(https://github\.com/[^"]+)"',
    )
    for candidate in candidates:
        for pattern in patterns:
            for match in re.findall(pattern, candidate, flags=re.IGNORECASE):
                normalized = normalize_github_url(match.replace('\\/', '/'))
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                urls.append(normalized)
    return tuple(urls)


def _iter_github_urls_from_json(value: object):
    if isinstance(value, str):
        normalized = normalize_github_url(value)
        if normalized:
            yield normalized
        for match in GITHUB_URL_PATTERN.findall(value):
            normalized = normalize_github_url(match.rstrip('),.;:!?'))
            if normalized:
                yield normalized
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_github_urls_from_json(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_github_urls_from_json(item)
