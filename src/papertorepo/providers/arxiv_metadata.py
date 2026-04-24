from __future__ import annotations

import asyncio
import html
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from typing import Any

import aiohttp

from papertorepo.core.http import RateLimiter, request_text
from papertorepo.core.normalize.arxiv import build_arxiv_abs_url, extract_arxiv_id, sanitize_title
from papertorepo.core.records import Paper


ATOM_NS = "http://www.w3.org/2005/Atom"
ARXIV_SCHEMA_NS = "http://arxiv.org/schemas/atom"
ARXIV_NS = {"a": ATOM_NS, "arxiv": ARXIV_SCHEMA_NS}


class ArxivMetadataClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        min_interval: float = 0.2,
        max_concurrent: int = 1,
        max_retries: int | None = None,
        rate_limiter: RateLimiter | None = None,
    ):
        self.session = session
        self.semaphore = asyncio.Semaphore(max(1, max_concurrent))
        self.rate_limiter = rate_limiter or RateLimiter(max(0.0, min_interval))
        self.max_retries = None if max_retries is None else max(0, max_retries)

    async def fetch_search_page(
        self,
        *,
        search_query: str,
        start: int = 0,
        max_results: int = 100,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={
                "search_query": search_query,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
                "start": str(start),
                "max_results": str(max_results),
            },
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv metadata query",
            max_retries=self.max_retries,
        )

    async def fetch_category_page(
        self,
        *,
        category: str,
        start: int = 0,
        max_results: int = 100,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await self.fetch_search_page(
            search_query=f"cat:{category}",
            start=start,
            max_results=max_results,
        )

    async def fetch_submitted_day_page(
        self,
        *,
        category: str,
        day: date,
        start: int = 0,
        max_results: int = 100,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        submitted_from = day.strftime("%Y%m%d0000")
        submitted_to = day.strftime("%Y%m%d2359")
        return await self.fetch_search_page(
            search_query=f"cat:{category} AND submittedDate:[{submitted_from} TO {submitted_to}]",
            start=start,
            max_results=max_results,
        )

    async def fetch_id_list_feed(self, arxiv_ids: list[str]) -> tuple[int | None, str | None, dict[str, str], str | None]:
        id_list = ",".join(item.strip() for item in arxiv_ids if item and item.strip())
        max_results = len([item for item in arxiv_ids if item and item.strip()])
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={
                "id_list": id_list,
                "max_results": str(max_results),
            },
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv metadata id_list query",
            max_retries=self.max_retries,
        )

    async def fetch_listing_page(
        self,
        *,
        category: str,
        period: str,
        skip: int = 0,
        show: int = 2000,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://arxiv.org/list/{category}/{period}",
            params={
                "skip": str(skip),
                "show": str(show),
            },
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv listing query",
            max_retries=self.max_retries,
        )

    async def fetch_catchup_page(
        self,
        *,
        category: str,
        day: date,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://arxiv.org/catchup/{category}/{day.isoformat()}",
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv catchup query",
            max_retries=self.max_retries,
        )

    async def fetch_paper_feed(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={"id_list": arxiv_id},
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv paper query",
            max_retries=self.max_retries,
        )


def parse_papers_from_feed(feed_xml: str) -> list[Paper]:
    if not feed_xml:
        return []

    try:
        root = ET.fromstring(feed_xml)
    except ET.ParseError:
        return []

    papers: list[Paper] = []
    for entry in root.findall("a:entry", ARXIV_NS):
        parsed = _parse_paper_entry(entry)
        if parsed is not None:
            papers.append(parsed)
    return papers


def parse_arxiv_ids_from_feed(feed_xml: str) -> list[str]:
    if not feed_xml:
        return []
    try:
        root = ET.fromstring(feed_xml)
    except ET.ParseError:
        return []

    arxiv_ids: list[str] = []
    seen: set[str] = set()
    for entry in root.findall("a:entry", ARXIV_NS):
        id_el = entry.find("a:id", ARXIV_NS)
        if id_el is None or not id_el.text:
            continue
        arxiv_id = extract_arxiv_id(id_el.text.strip())
        if not arxiv_id or arxiv_id in seen:
            continue
        seen.add(arxiv_id)
        arxiv_ids.append(arxiv_id)
    return arxiv_ids


def _parse_paper_entry(entry: ET.Element) -> Paper | None:
    id_el = entry.find("a:id", ARXIV_NS)
    title_el = entry.find("a:title", ARXIV_NS)
    summary_el = entry.find("a:summary", ARXIV_NS)
    if id_el is None or title_el is None or summary_el is None or not id_el.text:
        return None

    entry_id = id_el.text.strip()
    arxiv_id = extract_arxiv_id(entry_id)
    if not arxiv_id:
        return None

    published_at = _parse_datetime(_child_text(entry, "a:published"))
    updated_at = _parse_datetime(_child_text(entry, "a:updated"))
    authors, author_details = _extract_authors(entry)
    categories, category_details = _extract_categories(entry)
    primary_category, primary_category_scheme = _extract_primary_category(entry, categories, category_details)
    links = _extract_links(entry)

    return Paper(
        arxiv_id=arxiv_id,
        entry_id=entry_id,
        abs_url=build_arxiv_abs_url(arxiv_id),
        title=sanitize_title("".join(title_el.itertext())),
        abstract=sanitize_title("".join(summary_el.itertext())),
        published_at=published_at,
        updated_at=updated_at,
        authors=authors,
        author_details=author_details,
        categories=categories,
        category_details=category_details,
        links=links,
        comment=_extract_arxiv_text(entry, "comment"),
        journal_ref=_extract_arxiv_text(entry, "journal_ref"),
        doi=_extract_arxiv_text(entry, "doi"),
        primary_category=primary_category,
        primary_category_scheme=primary_category_scheme,
    )


def _child_text(entry: ET.Element, path: str) -> str | None:
    child = entry.find(path, ARXIV_NS)
    if child is None or child.text is None:
        return None
    value = " ".join(child.text.split()).strip()
    return html.unescape(value) if value else None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_arxiv_text(entry: ET.Element, field_name: str) -> str | None:
    value = _child_text(entry, f"arxiv:{field_name}")
    if value:
        return value
    suffix = f"}}{field_name}"
    for child in entry:
        if child.tag.endswith(suffix) and child.text:
            normalized = " ".join(child.text.split()).strip()
            return html.unescape(normalized) if normalized else None
    return None


def _extract_links(entry: ET.Element) -> tuple[dict[str, Any], ...]:
    links: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
    for link in entry.findall("a:link", ARXIV_NS):
        href = (link.attrib.get("href") or "").strip() or None
        rel = (link.attrib.get("rel") or "").strip() or None
        type_value = (link.attrib.get("type") or "").strip() or None
        title = (link.attrib.get("title") or "").strip() or None
        key = (href, rel, type_value, title)
        if href is None or key in seen:
            continue
        seen.add(key)
        links.append(
            {
                "href": href,
                "rel": rel,
                "type": type_value,
                "title": title,
            }
        )
    return tuple(links)


def _extract_categories(entry: ET.Element) -> tuple[tuple[str, ...], tuple[dict[str, Any], ...]]:
    terms: list[str] = []
    term_seen: set[str] = set()
    details: list[dict[str, Any]] = []
    detail_seen: set[tuple[str | None, str | None, str | None]] = set()

    for category in entry.findall("a:category", ARXIV_NS):
        term = (category.attrib.get("term") or "").strip() or None
        scheme = (category.attrib.get("scheme") or "").strip() or None
        label = (category.attrib.get("label") or "").strip() or None
        if term and term not in term_seen:
            term_seen.add(term)
            terms.append(term)
        detail_key = (term, scheme, label)
        if detail_key in detail_seen:
            continue
        detail_seen.add(detail_key)
        details.append(
            {
                "term": term,
                "scheme": scheme,
                "label": label,
            }
        )
    return tuple(terms), tuple(details)


def _extract_primary_category(
    entry: ET.Element,
    categories: tuple[str, ...],
    category_details: tuple[dict[str, Any], ...],
) -> tuple[str | None, str | None]:
    primary = entry.find("arxiv:primary_category", ARXIV_NS)
    if primary is not None:
        term = (primary.attrib.get("term") or "").strip() or None
        scheme = (primary.attrib.get("scheme") or "").strip() or None
        if term or scheme:
            return term, scheme
    fallback_term = categories[0] if categories else None
    fallback_scheme = None
    for item in category_details:
        if item.get("term") == fallback_term:
            fallback_scheme = item.get("scheme")
            break
    return fallback_term, fallback_scheme


def _extract_authors(entry: ET.Element) -> tuple[tuple[str, ...], tuple[dict[str, Any], ...]]:
    names: list[str] = []
    details: list[dict[str, Any]] = []
    for author in entry.findall("a:author", ARXIV_NS):
        name_value = _child_text(author, "a:name")
        if not name_value:
            continue
        names.append(name_value)
        affiliations: list[str] = []
        seen_affiliations: set[str] = set()
        for child in author:
            if not child.tag.endswith("}affiliation") or not child.text:
                continue
            affiliation = html.unescape(" ".join(child.text.split()).strip())
            if not affiliation or affiliation in seen_affiliations:
                continue
            seen_affiliations.add(affiliation)
            affiliations.append(affiliation)
        details.append(
            {
                "name": name_value,
                "affiliations": affiliations,
            }
        )
    return tuple(names), tuple(details)
