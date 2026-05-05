from __future__ import annotations

import re
from urllib.parse import urlparse


GITHUB_HOSTS = {"github.com", "www.github.com"}
GITHUB_REPO_URL_PATTERN = re.compile(
    r"https?://(?:www\.)?github\.com/[\w.-]+/[\w.-]+(?:/[^\s\"'<>)]*)?",
    re.IGNORECASE,
)
_RESERVED_OWNER_SEGMENTS = {
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


def extract_owner_repo(github_url: str) -> tuple[str, str] | None:
    if not github_url or not isinstance(github_url, str):
        return None

    parsed = urlparse(github_url.strip())
    host = (parsed.hostname or parsed.netloc or "").lower()
    if parsed.scheme not in {"http", "https"} or host not in GITHUB_HOSTS:
        return None

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None
    owner, repo = parts[0], parts[1]
    repo = re.sub(r"\.git$", "", repo, flags=re.IGNORECASE)
    if not owner or not repo:
        return None
    if owner.lower() in _RESERVED_OWNER_SEGMENTS:
        return None
    return owner, repo


def is_valid_github_repo_url(url: str) -> bool:
    return extract_owner_repo(url) is not None


def normalize_github_url(url: str) -> str | None:
    result = extract_owner_repo(url)
    if not result:
        return None
    owner, repo = result
    owner = owner.lower()
    repo = repo.lower()
    return f"https://github.com/{owner}/{repo}"


def extract_github_repo_urls(text: str) -> tuple[str, ...]:
    if not text or not isinstance(text, str):
        return ()

    urls: list[str] = []
    seen: set[str] = set()
    for match in GITHUB_REPO_URL_PATTERN.findall(text):
        cleaned = match.rstrip("),.;:!?'\"")
        normalized = normalize_github_url(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return tuple(urls)
