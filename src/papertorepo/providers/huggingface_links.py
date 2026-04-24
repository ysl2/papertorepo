from __future__ import annotations

import asyncio
import json

import aiohttp

from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.github import normalize_github_url


class HuggingFaceLinksClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        huggingface_token: str = "",
        min_interval: float = 0.2,
        max_concurrent: int = 2,
    ):
        self.session = session
        self.huggingface_token = huggingface_token
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(max(0.0, min_interval))

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

    def _build_headers(self, accept: str) -> dict[str, str]:
        headers = {
            "Accept": accept,
            "User-Agent": "papertorepo",
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
