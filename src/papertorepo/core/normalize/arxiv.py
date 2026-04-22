from __future__ import annotations

import html
import re
from urllib.parse import urlparse


ARXIV_HOSTS = {"arxiv.org", "www.arxiv.org"}
ARXIV_URL_PATTERN = re.compile(
    r"arxiv\.org/(?:abs|pdf)/([0-9]{4}\.[0-9]{4,5})(?:v\d+)?(?:\.pdf)?",
    re.IGNORECASE,
)
ARXIV_SINGLE_PAPER_PATTERN = re.compile(
    r"^/(?:abs|pdf)/(?P<id>[0-9]{4}\.[0-9]{4,5})(?:v\d+)?(?:\.pdf)?/?$",
    re.IGNORECASE,
)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
NON_ALNUM_PATTERN = re.compile(r"[^0-9a-z]+")


def extract_arxiv_id(url: str) -> str | None:
    if not url or not isinstance(url, str):
        return None
    match = ARXIV_URL_PATTERN.search(url.strip())
    if not match:
        return None
    return match.group(1)


def extract_arxiv_id_from_single_paper_url(url: str) -> str | None:
    if not url or not isinstance(url, str):
        return None

    parsed = urlparse(url.strip())
    host = (parsed.hostname or parsed.netloc or "").lower()
    path = re.sub(r"/+", "/", parsed.path or "")
    if parsed.scheme not in {"http", "https"} or host not in ARXIV_HOSTS:
        return None

    match = ARXIV_SINGLE_PAPER_PATTERN.fullmatch(path)
    if not match:
        return None
    return match.group("id")


def build_arxiv_abs_url(arxiv_id: str) -> str:
    return f"https://arxiv.org/abs/{arxiv_id}"


def normalize_arxiv_url(url: str) -> str | None:
    arxiv_id = extract_arxiv_id(url)
    if not arxiv_id:
        return None
    return build_arxiv_abs_url(arxiv_id)


def normalize_title_for_matching(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    normalized = sanitize_title(title).casefold()
    normalized = NON_ALNUM_PATTERN.sub(" ", normalized)
    return " ".join(normalized.split()).strip()


def sanitize_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    stripped = HTML_TAG_PATTERN.sub(" ", title)
    return html.unescape(" ".join(stripped.split())).strip()
