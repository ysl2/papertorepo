from __future__ import annotations

import asyncio
import html
import json
import re

import aiohttp

from src.ghstars.net.http import RateLimiter, request_text
from src.ghstars.normalize.arxiv import normalize_title_for_matching
from src.ghstars.normalize.github import extract_github_repo_urls, normalize_github_url


HUGGINGFACE_PAPER_ID_PATTERN = re.compile(r"^[0-9]{4}\.[0-9]{4,5}$")
HUGGINGFACE_SEARCH_MAX_CONCURRENT = 1


class HuggingFaceLinksClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        huggingface_token: str = "",
        min_interval: float = 0.5,
        max_concurrent: int = 2,
    ):
        self.session = session
        self.huggingface_token = huggingface_token
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.search_semaphore = asyncio.Semaphore(HUGGINGFACE_SEARCH_MAX_CONCURRENT)
        self.rate_limiter = RateLimiter(max(min_interval, 0.5))

    async def fetch_paper_payload(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://huggingface.co/api/papers/{arxiv_id}",
            headers=self._build_headers("application/json"),
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="Hugging Face Papers API",
            allowed_statuses={404},
        )

    async def fetch_paper_html(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        async with self.search_semaphore:
            return await request_text(
                self.session,
                f"https://huggingface.co/papers/{arxiv_id}",
                headers=self._build_headers("text/html,application/json"),
                semaphore=self.semaphore,
                rate_limiter=self.rate_limiter,
                retry_prefix="Hugging Face Papers HTML",
                allowed_statuses={404},
            )

    async def fetch_search_html(self, title: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        async with self.search_semaphore:
            return await request_text(
                self.session,
                "https://huggingface.co/papers",
                headers=self._build_headers("text/html,application/json"),
                params={"q": title},
                semaphore=self.semaphore,
                rate_limiter=self.rate_limiter,
                retry_prefix="Hugging Face Papers search",
            )

    async def fetch_search_payload(self, title: str, *, limit: int = 10) -> tuple[int | None, str | None, dict[str, str], str | None]:
        async with self.search_semaphore:
            return await request_text(
                self.session,
                "https://huggingface.co/api/papers/search",
                headers=self._build_headers("application/json"),
                params={"q": title, "limit": str(limit)},
                semaphore=self.semaphore,
                rate_limiter=self.rate_limiter,
                retry_prefix="Hugging Face Papers search API",
            )

    def _build_headers(self, accept: str) -> dict[str, str]:
        headers = {
            "Accept": accept,
            "User-Agent": "scripts.ghstars-ng",
        }
        if self.huggingface_token:
            headers["Authorization"] = f"Bearer {self.huggingface_token}"
        return headers


def extract_github_url_from_hf_payload(payload_text: str | None) -> tuple[str, ...]:
    if not payload_text or not isinstance(payload_text, str):
        return ()
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ()
    if not isinstance(payload, dict):
        return ()

    github_url = payload.get("githubRepo")
    normalized = normalize_github_url(github_url) if isinstance(github_url, str) else None
    if not normalized:
        return ()
    return (normalized,)


def extract_github_url_from_hf_html(html_text: str | None) -> tuple[str, ...]:
    if not html_text or not isinstance(html_text, str):
        return ()

    candidates = [html_text]
    decoded_html = html.unescape(html_text)
    if decoded_html != html_text:
        candidates.insert(0, decoded_html)

    urls: list[str] = []
    seen: set[str] = set()
    patterns = (
        r'"githubRepo"\s*:\s*"(https://github\.com/[^"]+)"',
        r'<a[^>]*href="(https://github\.com/[^"]+)"[^>]*\b(?:aria-label|title)="GitHub"[^>]*>',
        r'<a[^>]*\b(?:aria-label|title)="GitHub"[^>]*href="(https://github\.com/[^"]+)"[^>]*>',
        r'href="(https://github\.com/[^"]+)"[^>]*>\s*GitHub\s*<',
        r'GitHub\s*</[^>]+>\s*<[^>]+href="(https://github\.com/[^"]+)"',
    )
    for candidate in candidates:
        for pattern in patterns:
            for match in re.findall(pattern, candidate, flags=re.IGNORECASE):
                normalized = normalize_github_url(match.replace('\\/', '/'))
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                urls.append(normalized)

    if urls:
        return tuple(urls)
    return extract_github_repo_urls(html_text)


def extract_paper_id_from_search_html(html_text: str, title_query: str) -> tuple[str | None, str | None]:
    match = re.search(r'data-target="DailyPapers"[^>]*data-props="([^"]*)"', html_text)
    if not match:
        return None, None
    try:
        payload = json.loads(html.unescape(match.group(1)))
    except json.JSONDecodeError:
        return None, None
    return _pick_best_paper_id(payload, title_query)


def extract_paper_id_from_search_payload(payload_text: str | None, title_query: str) -> tuple[str | None, str | None]:
    if not payload_text or not isinstance(payload_text, str):
        return None, None
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    return _pick_best_paper_id(payload, title_query)


def _pick_best_paper_id(payload: dict[str, object], title_query: str) -> tuple[str | None, str | None]:
    title_query_norm = normalize_title_for_matching(title_query)
    best_id = None
    best_score = -1
    best_source = None
    for item in _iter_search_items(payload):
        paper_id = str(item.get("paper_id") or "").strip()
        title = normalize_title_for_matching(str(item.get("title") or ""))
        if not HUGGINGFACE_PAPER_ID_PATTERN.match(paper_id) or not title:
            continue
        score = 0
        source = None
        if title == title_query_norm:
            score = 100
            source = "title_search_huggingface_exact"
        elif title_query_norm in title:
            score = 80
            source = "title_search_huggingface_contained"
        elif title in title_query_norm:
            score = 60
            source = "title_search_huggingface_contains_entry"
        if score > 0 and score > best_score:
            best_score = score
            best_id = paper_id
            best_source = source
    return best_id, best_source


def _iter_search_items(payload: dict[str, object]) -> list[dict[str, str]]:
    items = payload.get("searchResults")
    if not isinstance(items, list) or not items:
        items = payload.get("dailyPapers")
    if not isinstance(items, list):
        return []

    output: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        paper = item.get("paper", {})
        if not isinstance(paper, dict):
            continue
        output.append(
            {
                "paper_id": str(paper.get("id") or "").strip(),
                "title": " ".join(str(item.get("title") or paper.get("title") or "").split()).strip(),
            }
        )
    return output
