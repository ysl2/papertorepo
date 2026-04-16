from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import uuid

import aiohttp

from src.ghstars.associate.resolver import build_final_links, parity_summary
from src.ghstars.config import load_config, resolve_categories
from src.ghstars.export.csv import build_export_row, write_papers_csv
from src.ghstars.net.http import build_timeout
from src.ghstars.normalize.github import normalize_github_url
from src.ghstars.providers.alphaxiv_links import (
    AlphaXivLinksClient,
    extract_github_url_from_alphaxiv_html,
    extract_github_url_from_alphaxiv_payload,
)
from src.ghstars.providers.arxiv_links import ArxivLinksClient, extract_github_urls_from_abs_html, extract_github_urls_from_comment
from src.ghstars.providers.arxiv_metadata import ArxivMetadataClient, parse_papers_from_feed
from src.ghstars.providers.github import GitHubClient
from src.ghstars.providers.huggingface_links import (
    HuggingFaceLinksClient,
    extract_github_url_from_hf_html,
    extract_github_url_from_hf_payload,
    extract_paper_id_from_search_html,
    extract_paper_id_from_search_payload,
)
from src.ghstars.models import Paper, PaperSyncLease
from src.ghstars.storage.db import Database, LeaseLostError
from src.ghstars.storage.raw_cache import RawCacheStore


EXTRACTOR_VERSION = "2"
DEFAULT_LATEST_MAX_RESULTS = 100
ARXIV_PAGE_SIZE = 100
EXACT_NO_MATCH_TTL = timedelta(days=7)
PAPER_SYNC_LEASE_TTL_SECONDS = 30.0
PAPER_SYNC_LEASE_HEARTBEAT_SECONDS = 10.0
RESOURCE_LEASE_TTL_SECONDS = 30.0
RESOURCE_LEASE_HEARTBEAT_SECONDS = 10.0


class ArxivWindowSyncIncompleteError(RuntimeError):
    pass


@dataclass(frozen=True)
class ArxivSyncWindow:
    start_date: date | None = None
    end_date: date | None = None

    @property
    def enabled(self) -> bool:
        return self.start_date is not None or self.end_date is not None

    def contains(self, published_date: date | None) -> bool:
        if published_date is None:
            return False
        if self.start_date is not None and published_date < self.start_date:
            return False
        if self.end_date is not None and published_date > self.end_date:
            return False
        return True

    def describe(self) -> str:
        if not self.enabled:
            return "latest"
        if self.start_date == self.end_date and self.start_date is not None:
            return self.start_date.isoformat()
        if self.start_date is None:
            return f"<= {self.end_date.isoformat()}"
        if self.end_date is None:
            return f">= {self.start_date.isoformat()}"
        return f"{self.start_date.isoformat()}..{self.end_date.isoformat()}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scripts.ghstars-ng")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    sync_arxiv = sync_subparsers.add_parser("arxiv")
    sync_arxiv.add_argument("--categories", default=None)
    sync_arxiv.add_argument("--max-results", type=int, default=None)
    sync_arxiv.add_argument("--from", dest="from_date", default=None)
    sync_arxiv.add_argument("--to", dest="to_date", default=None)
    sync_arxiv.add_argument("--day", default=None)
    sync_arxiv.add_argument("--month", default=None)

    sync_links = sync_subparsers.add_parser("links")
    sync_links.add_argument("--categories", default=None)
    sync_links.add_argument("--concurrency", type=int, default=None)
    sync_links.add_argument("--from", dest="from_date", default=None)
    sync_links.add_argument("--to", dest="to_date", default=None)
    sync_links.add_argument("--day", default=None)
    sync_links.add_argument("--month", default=None)

    audit_parser = subparsers.add_parser("audit")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)
    audit_parity = audit_subparsers.add_parser("parity")
    audit_parity.add_argument("--categories", default=None)
    audit_parity.add_argument("--from", dest="from_date", default=None)
    audit_parity.add_argument("--to", dest="to_date", default=None)
    audit_parity.add_argument("--day", default=None)
    audit_parity.add_argument("--month", default=None)

    enrich_parser = subparsers.add_parser("enrich")
    enrich_subparsers = enrich_parser.add_subparsers(dest="enrich_command", required=True)
    enrich_repos = enrich_subparsers.add_parser("repos")
    enrich_repos.add_argument("--categories", default=None)
    enrich_repos.add_argument("--from", dest="from_date", default=None)
    enrich_repos.add_argument("--to", dest="to_date", default=None)
    enrich_repos.add_argument("--day", default=None)
    enrich_repos.add_argument("--month", default=None)

    export_parser = subparsers.add_parser("export")
    export_subparsers = export_parser.add_subparsers(dest="export_command", required=True)
    export_csv = export_subparsers.add_parser("csv")
    export_csv.add_argument("--categories", default=None)
    export_csv.add_argument("--output", required=True)
    export_csv.add_argument("--from", dest="from_date", default=None)
    export_csv.add_argument("--to", dest="to_date", default=None)
    export_csv.add_argument("--day", default=None)
    export_csv.add_argument("--month", default=None)

    return parser


async def async_main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config()
    categories = resolve_categories(getattr(args, "categories", None), config.default_categories)

    database = Database(config.db_path)
    raw_cache = RawCacheStore(config.raw_dir)
    try:
        async with aiohttp.ClientSession(timeout=build_timeout()) as session:
            arxiv_metadata = ArxivMetadataClient(session, min_interval=config.arxiv_api_min_interval)
            arxiv_links = ArxivLinksClient(session, min_interval=config.arxiv_api_min_interval)
            huggingface = HuggingFaceLinksClient(
                session,
                huggingface_token=config.huggingface_token,
                min_interval=config.huggingface_min_interval,
            )
            alphaxiv = AlphaXivLinksClient(
                session,
                alphaxiv_token=config.alphaxiv_token,
                min_interval=0.5,
            )
            github = GitHubClient(session, github_token=config.github_token, min_interval=config.github_min_interval)

            if args.command == "sync" and args.sync_command == "arxiv":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_sync_arxiv(
                    database,
                    raw_cache,
                    arxiv_metadata,
                    categories,
                    max_results=args.max_results,
                    window=window,
                )
                return 0
            if args.command == "sync" and args.sync_command == "links":
                concurrency = _resolve_sync_links_concurrency(args.concurrency, config.sync_links_concurrency)
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_sync_links(
                    database,
                    raw_cache,
                    arxiv_links,
                    huggingface,
                    alphaxiv,
                    categories,
                    concurrency=concurrency,
                    window=window,
                )
                return 0
            if args.command == "audit" and args.audit_command == "parity":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                _run_audit_parity(database, categories, window=window)
                return 0
            if args.command == "enrich" and args.enrich_command == "repos":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_enrich_repos(database, github, categories, window=window)
                return 0
            if args.command == "export" and args.export_command == "csv":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                _run_export_csv(database, categories, Path(args.output), window=window)
                return 0
    finally:
        database.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


async def _run_sync_arxiv(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivMetadataClient,
    categories: tuple[str, ...],
    *,
    max_results: int | None,
    window: ArxivSyncWindow,
) -> None:
    owner_id = _build_sync_owner_id()
    for category in categories:
        stream_name = f"arxiv:{category}:{window.describe()}" if window.enabled else f"arxiv:{category}"
        stream_lease = database.try_acquire_resource_lease(
            stream_name,
            owner_id=owner_id,
            lease_token=str(uuid.uuid4()),
            lease_ttl_seconds=RESOURCE_LEASE_TTL_SECONDS,
        )
        if stream_lease is None:
            print(f"{category}: skipped (stream sync held by another process)")
            continue

        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(_heartbeat_resource_lease(database, stream_lease, stop_heartbeat))
        try:
            if window.enabled:
                synced_count, latest_cursor = await _sync_arxiv_category_by_window(
                    database,
                    raw_cache,
                    client,
                    category,
                    window,
                )
                _ensure_resource_lease(database, stream_lease)
                database.set_sync_state(
                    stream_name,
                    latest_cursor,
                    lease_owner_id=stream_lease.owner_id,
                    lease_token=stream_lease.lease_token,
                )
                print(f"{category}: synced {synced_count} papers in {window.describe()}")
                continue

            latest_max_results = max_results or DEFAULT_LATEST_MAX_RESULTS
            search_query = f"cat:{category}"
            status, body, headers, error = await client.fetch_search_page(
                search_query=search_query,
                start=0,
                max_results=latest_max_results,
            )
            if error or body is None or status is None:
                print(f"{category}: {error or 'empty response'}")
                continue
            request_key = f"search={search_query}:start=0:max_results={latest_max_results}"
            raw_entry = _store_arxiv_search_page(
                database,
                raw_cache,
                search_query=search_query,
                request_key=request_key,
                status=status,
                body=body,
                headers=headers,
            )
            papers = parse_papers_from_feed(body)
            _persist_arxiv_papers(database, raw_entry.id, papers, surface="search_feed")
            _ensure_resource_lease(database, stream_lease)
            database.set_sync_state(
                stream_name,
                papers[0].updated_at if papers else None,
                lease_owner_id=stream_lease.owner_id,
                lease_token=stream_lease.lease_token,
            )
            print(f"{category}: synced {len(papers)} papers")
        finally:
            stop_heartbeat.set()
            await heartbeat_task
            database.release_resource_lease(
                stream_name,
                owner_id=stream_lease.owner_id,
                lease_token=stream_lease.lease_token,
            )


async def _sync_arxiv_category_by_window(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivMetadataClient,
    category: str,
    window: ArxivSyncWindow,
) -> tuple[int, str | None]:
    start = 0
    synced_count = 0
    latest_cursor = None
    search_query = _build_arxiv_window_search_query(category, window)

    while True:
        status, body, headers, error = await client.fetch_search_page(
            search_query=search_query,
            start=start,
            max_results=ARXIV_PAGE_SIZE,
        )
        if error or body is None or status is None:
            raise ArxivWindowSyncIncompleteError(
                f"{category}: arXiv window sync {window.describe()} incomplete at start={start} "
                f"({error or 'empty response'}); aborting after persisting fetched pages"
            )

        request_key = f"search={search_query}:start={start}:max_results={ARXIV_PAGE_SIZE}"
        raw_entry = _store_arxiv_search_page(
            database,
            raw_cache,
            search_query=search_query,
            request_key=request_key,
            status=status,
            body=body,
            headers=headers,
        )
        papers = parse_papers_from_feed(body)
        if not papers:
            break
        if latest_cursor is None:
            latest_cursor = papers[0].updated_at

        _persist_arxiv_papers(database, raw_entry.id, papers, surface="search_feed")
        synced_count += len(papers)
        if len(papers) < ARXIV_PAGE_SIZE:
            break
        start += ARXIV_PAGE_SIZE

    return synced_count, latest_cursor


def _store_arxiv_search_page(
    database: Database,
    raw_cache: RawCacheStore,
    *,
    search_query: str,
    request_key: str,
    status: int,
    body: str,
    headers: dict[str, str],
):
    path, content_hash = raw_cache.write_body(
        provider="arxiv",
        surface="search_feed",
        request_key=request_key,
        body=body,
        content_type=headers.get("Content-Type"),
    )
    return database.upsert_raw_cache(
        provider="arxiv",
        surface="search_feed",
        request_key=request_key,
        request_url=f"https://export.arxiv.org/api/query?search_query={search_query}",
        content_type=headers.get("Content-Type"),
        status_code=status,
        body_path=path,
        content_hash=content_hash,
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    )


def _persist_arxiv_papers(database: Database, raw_cache_id: int, papers, *, surface: str) -> None:
    for paper in papers:
        database.persist_paper_with_source(
            paper,
            provider="arxiv",
            surface=surface,
            raw_cache_id=raw_cache_id,
            data={
                "title": paper.title,
                "abstract": paper.abstract,
                "categories": list(paper.categories),
                "comment": paper.comment,
            },
        )


def _format_arxiv_datetime(value: date, *, end_of_day: bool) -> str:
    return f"{value.strftime('%Y%m%d')}{'2359' if end_of_day else '0000'}"


def _build_arxiv_window_search_query(category: str, window: ArxivSyncWindow) -> str:
    start_date = window.start_date or date(1991, 1, 1)
    end_date = window.end_date or date.today()
    return (
        f"cat:{category} AND submittedDate:[{_format_arxiv_datetime(start_date, end_of_day=False)} "
        f"TO {_format_arxiv_datetime(end_date, end_of_day=True)}]"
    )


def _resolve_arxiv_sync_window(*, day: str | None, month: str | None, from_date: str | None, to_date: str | None) -> ArxivSyncWindow:
    specified = sum(bool(value) for value in (day, month, from_date, to_date))
    if day and (month or from_date or to_date):
        raise ValueError("--day cannot be combined with --month/--from/--to")
    if month and (day or from_date or to_date):
        raise ValueError("--month cannot be combined with --day/--from/--to")
    if specified == 0:
        return ArxivSyncWindow()
    if day:
        parsed_day = date.fromisoformat(day)
        return ArxivSyncWindow(start_date=parsed_day, end_date=parsed_day)
    if month:
        month_start = datetime.strptime(month, "%Y-%m").date()
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return ArxivSyncWindow(start_date=month_start, end_date=next_month - timedelta(days=1))
    start_date = date.fromisoformat(from_date) if from_date else None
    end_date = date.fromisoformat(to_date) if to_date else None
    if start_date is None and end_date is None:
        return ArxivSyncWindow()
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("--from must be <= --to")
    return ArxivSyncWindow(start_date=start_date, end_date=end_date)


def _resolve_sync_links_concurrency(cli_value: int | None, default: int) -> int:
    value = cli_value if cli_value is not None else default
    if value <= 0:
        raise ValueError("--concurrency must be >= 1")
    return value


def _list_papers_for_window(database: Database, categories: tuple[str, ...], *, window: ArxivSyncWindow) -> list[Paper]:
    return database.list_papers_by_categories(
        categories,
        published_from=window.start_date.isoformat() if window.start_date is not None else None,
        published_to=window.end_date.isoformat() if window.end_date is not None else None,
    )


async def _run_sync_links(
    database: Database,
    raw_cache: RawCacheStore,
    arxiv_links: ArxivLinksClient,
    huggingface: HuggingFaceLinksClient,
    alphaxiv: AlphaXivLinksClient,
    categories: tuple[str, ...],
    *,
    concurrency: int = 1,
    window: ArxivSyncWindow = ArxivSyncWindow(),
) -> None:
    papers = _list_papers_for_window(database, categories, window=window)
    if not papers:
        return
    owner_id = _build_sync_owner_id()
    if concurrency == 1:
        for paper in papers:
            await _sync_links_for_paper(
                database,
                raw_cache,
                arxiv_links,
                huggingface,
                alphaxiv,
                paper,
                owner_id=owner_id,
            )
        return

    queue: asyncio.Queue[Paper | None] = asyncio.Queue()
    for paper in papers:
        queue.put_nowait(paper)
    worker_count = min(concurrency, len(papers))
    for _ in range(worker_count):
        queue.put_nowait(None)

    async def worker() -> None:
        worker_db = Database(database.db_path)
        try:
            while True:
                paper = await queue.get()
                if paper is None:
                    return
                try:
                    await _sync_links_for_paper(
                        worker_db,
                        raw_cache,
                        arxiv_links,
                        huggingface,
                        alphaxiv,
                        paper,
                        owner_id=owner_id,
                    )
                except Exception as exc:
                    print(f"{paper.arxiv_id}: sync links failed ({exc})")
        finally:
            worker_db.close()

    await asyncio.gather(*(worker() for _ in range(worker_count)))


async def _sync_links_for_paper(
    database: Database,
    raw_cache: RawCacheStore,
    arxiv_links: ArxivLinksClient,
    huggingface: HuggingFaceLinksClient,
    alphaxiv: AlphaXivLinksClient,
    paper: Paper,
    *,
    owner_id: str,
) -> None:
    lease = database.try_acquire_paper_sync_lease(
        paper.arxiv_id,
        owner_id=owner_id,
        lease_token=str(uuid.uuid4()),
        lease_ttl_seconds=PAPER_SYNC_LEASE_TTL_SECONDS,
    )
    if lease is None:
        print(f"{paper.arxiv_id}: skipped (lease held by another sync)")
        return

    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(_heartbeat_paper_sync_lease(database, lease, stop_heartbeat))
    try:
        await _sync_arxiv_link_surfaces(database, raw_cache, arxiv_links, paper.arxiv_id, paper.comment, paper.title, lease)
        _ensure_paper_lease(database, lease)
        observations = database.list_repo_observations(paper.arxiv_id)
        if not _has_found_repo(observations):
            await _sync_huggingface_exact_surfaces(database, raw_cache, huggingface, paper.arxiv_id, paper.title, lease)
            _ensure_paper_lease(database, lease)
            observations = database.list_repo_observations(paper.arxiv_id)
        if not _has_found_repo(observations):
            await _sync_alphaxiv_link_surfaces(database, raw_cache, alphaxiv, paper.arxiv_id, paper.title, lease)
            _ensure_paper_lease(database, lease)
            observations = database.list_repo_observations(paper.arxiv_id)

        final_links = build_final_links(paper.arxiv_id, observations)
        database.replace_paper_repo_links(
            paper.arxiv_id,
            final_links,
            lease_owner_id=lease.owner_id,
            lease_token=lease.lease_token,
        )
        print(f"{paper.arxiv_id}: {len(final_links)} final links")
    except LeaseLostError:
        print(f"{paper.arxiv_id}: skipped after lease loss")
    finally:
        stop_heartbeat.set()
        await heartbeat_task
        database.release_paper_sync_lease(
            paper.arxiv_id,
            owner_id=lease.owner_id,
            lease_token=lease.lease_token,
        )


async def _sync_arxiv_link_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivLinksClient,
    arxiv_id: str,
    comment: str | None,
    title: str,
    lease: PaperSyncLease,
) -> None:
    comment_urls = extract_github_urls_from_comment(comment)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="arxiv",
        surface="comment",
        urls=comment_urls,
        evidence_text=comment,
        raw_cache_id=None,
        lease=lease,
    )

    handled, _found, _cached_status = _try_reuse_exact_surface(
        database,
        raw_cache,
        arxiv_id=arxiv_id,
        provider="arxiv",
        surface="abs_html",
        extract_urls=extract_github_urls_from_abs_html,
        lease=lease,
    )
    if handled:
        return

    status, body, headers, error = await client.fetch_abs_html(arxiv_id)
    _ensure_paper_lease(database, lease)
    if error or status is None:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="arxiv",
            surface="abs_html",
            urls=(),
            evidence_text=title,
            raw_cache_id=None,
            empty_status="fetch_failed",
            error_message=error or "empty response",
            lease=lease,
        )
        return

    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="arxiv",
        surface="abs_html",
        request_key=f"abs:{arxiv_id}",
        request_url=f"https://arxiv.org/abs/{arxiv_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    urls = extract_github_urls_from_abs_html(body)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="arxiv",
        surface="abs_html",
        urls=urls,
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )


async def _sync_huggingface_exact_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: HuggingFaceLinksClient,
    arxiv_id: str,
    title: str,
    lease: PaperSyncLease,
) -> None:
    await _sync_huggingface_paper_surfaces(
        database,
        raw_cache,
        client,
        source_paper_id=arxiv_id,
        fetch_paper_id=arxiv_id,
        title=title,
        payload_surface="paper_api",
        html_surface="paper_html",
        lease=lease,
    )


async def _sync_huggingface_title_search_surfaces(database: Database, raw_cache: RawCacheStore, client: HuggingFaceLinksClient, arxiv_id: str, title: str) -> None:
    status, body, headers, error = await client.fetch_search_payload(title, limit=10)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="huggingface",
        surface="search_api",
        request_key=f"search_api:{title}",
        request_url="https://huggingface.co/api/papers/search",
        status=status,
        headers=headers,
        body=body,
    )
    if error:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_api_paper_api",
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
        )
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_api_paper_html",
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
        )
    else:
        matched_id, _source = extract_paper_id_from_search_payload(body, title)
        if matched_id and matched_id != arxiv_id:
            await _sync_huggingface_paper_surfaces(
                database,
                raw_cache,
                client,
                source_paper_id=arxiv_id,
                fetch_paper_id=matched_id,
                title=title,
                payload_surface="search_api_paper_api",
                html_surface="search_api_paper_html",
            )
            if _has_found_repo(database.list_repo_observations(arxiv_id)):
                return
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_api_paper_api",
            urls=(),
            evidence_text=body if body is not None else title,
            raw_cache_id=raw_cache_id,
        )
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_api_paper_html",
            urls=(),
            evidence_text=body if body is not None else title,
            raw_cache_id=raw_cache_id,
        )

    status, body, headers, error = await client.fetch_search_html(title)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="huggingface",
        surface="search_html",
        request_key=f"search_html:{title}",
        request_url="https://huggingface.co/papers",
        status=status,
        headers=headers,
        body=body,
    )
    if error:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_html_paper_api",
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
        )
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="huggingface",
            surface="search_html_paper_html",
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
        )
        return

    matched_id, _source = extract_paper_id_from_search_html(body or "", title)
    if matched_id and matched_id != arxiv_id:
        await _sync_huggingface_paper_surfaces(
            database,
            raw_cache,
            client,
            source_paper_id=arxiv_id,
            fetch_paper_id=matched_id,
            title=title,
            payload_surface="search_html_paper_api",
            html_surface="search_html_paper_html",
        )
        if _has_found_repo(database.list_repo_observations(arxiv_id)):
            return

    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="huggingface",
        surface="search_html_paper_api",
        urls=(),
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
    )
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="huggingface",
        surface="search_html_paper_html",
        urls=(),
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
    )


async def _sync_huggingface_paper_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: HuggingFaceLinksClient,
    *,
    source_paper_id: str,
    fetch_paper_id: str,
    title: str,
    payload_surface: str,
    html_surface: str,
    lease: PaperSyncLease | None = None,
) -> None:
    handled, found, cached_status = _try_reuse_exact_surface(
        database,
        raw_cache,
        arxiv_id=source_paper_id,
        provider="huggingface",
        surface=payload_surface,
        extract_urls=extract_github_url_from_hf_payload,
        lease=lease,
    )
    if handled:
        if found:
            return
        if cached_status == 404:
            return
    else:
        status, body, headers, error = await client.fetch_paper_payload(fetch_paper_id)
        _ensure_paper_lease(database, lease)
        raw_cache_id = _persist_raw_response(
            database,
            raw_cache,
            provider="huggingface",
            surface="paper_api",
            request_key=f"paper_api:{fetch_paper_id}",
            request_url=f"https://huggingface.co/api/papers/{fetch_paper_id}",
            status=status,
            headers=headers,
            body=body,
        )
        _ensure_paper_lease(database, lease)
        if error and status != 404:
            _replace_surface_observations(
                database,
                arxiv_id=source_paper_id,
                provider="huggingface",
                surface=payload_surface,
                urls=(),
                evidence_text=title,
                raw_cache_id=raw_cache_id,
                empty_status="fetch_failed",
                error_message=error,
                lease=lease,
            )
        else:
            payload_urls = extract_github_url_from_hf_payload(body)
            _replace_surface_observations(
                database,
                arxiv_id=source_paper_id,
                provider="huggingface",
                surface=payload_surface,
                urls=payload_urls,
                evidence_text=body if body is not None else title,
                raw_cache_id=raw_cache_id,
                lease=lease,
            )
            if payload_urls:
                return
            if status == 404:
                return

    handled, _found, _cached_status = _try_reuse_exact_surface(
        database,
        raw_cache,
        arxiv_id=source_paper_id,
        provider="huggingface",
        surface=html_surface,
        extract_urls=extract_github_url_from_hf_html,
        lease=lease,
    )
    if handled:
        return

    status, body, headers, error = await client.fetch_paper_html(fetch_paper_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="huggingface",
        surface="paper_html",
        request_key=f"paper_html:{fetch_paper_id}",
        request_url=f"https://huggingface.co/papers/{fetch_paper_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        _replace_surface_observations(
            database,
            arxiv_id=source_paper_id,
            provider="huggingface",
            surface=html_surface,
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
            lease=lease,
        )
        return

    _replace_surface_observations(
        database,
        arxiv_id=source_paper_id,
        provider="huggingface",
        surface=html_surface,
        urls=extract_github_url_from_hf_html(body),
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )


async def _sync_alphaxiv_link_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: AlphaXivLinksClient,
    arxiv_id: str,
    title: str,
    lease: PaperSyncLease,
) -> None:
    handled, found, cached_status = _try_reuse_exact_surface(
        database,
        raw_cache,
        arxiv_id=arxiv_id,
        provider="alphaxiv",
        surface="paper_api",
        extract_urls=extract_github_url_from_alphaxiv_payload,
        lease=lease,
    )
    if handled:
        if found:
            return
        if cached_status == 404:
            return
    else:
        status, body, headers, error = await client.fetch_paper_payload(arxiv_id)
        _ensure_paper_lease(database, lease)
        raw_cache_id = _persist_raw_response(
            database,
            raw_cache,
            provider="alphaxiv",
            surface="paper_api",
            request_key=f"paper_api:{arxiv_id}",
            request_url=f"https://api.alphaxiv.org/papers/v3/{arxiv_id}",
            status=status,
            headers=headers,
            body=body,
        )
        _ensure_paper_lease(database, lease)
        if error and status != 404:
            _replace_surface_observations(
                database,
                arxiv_id=arxiv_id,
                provider="alphaxiv",
                surface="paper_api",
                urls=(),
                evidence_text=title,
                raw_cache_id=raw_cache_id,
                empty_status="fetch_failed",
                error_message=error,
                lease=lease,
            )
        else:
            payload_urls = extract_github_url_from_alphaxiv_payload(body)
            _replace_surface_observations(
                database,
                arxiv_id=arxiv_id,
                provider="alphaxiv",
                surface="paper_api",
                urls=payload_urls,
                evidence_text=body if body is not None else title,
                raw_cache_id=raw_cache_id,
                lease=lease,
            )
            if payload_urls:
                return
            if status == 404:
                return

    handled, _found, _cached_status = _try_reuse_exact_surface(
        database,
        raw_cache,
        arxiv_id=arxiv_id,
        provider="alphaxiv",
        surface="paper_html",
        extract_urls=extract_github_url_from_alphaxiv_html,
        lease=lease,
    )
    if handled:
        return

    status, body, headers, error = await client.fetch_paper_html(arxiv_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="alphaxiv",
        surface="paper_html",
        request_key=f"paper_html:{arxiv_id}",
        request_url=f"https://www.alphaxiv.org/abs/{arxiv_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="alphaxiv",
            surface="paper_html",
            urls=(),
            evidence_text=title,
            raw_cache_id=raw_cache_id,
            empty_status="fetch_failed",
            error_message=error,
            lease=lease,
        )
        return

    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="alphaxiv",
        surface="paper_html",
        urls=extract_github_url_from_alphaxiv_html(body),
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )


async def _sync_github_title_search_surface(database: Database, client: GitHubRepositorySearchClient, arxiv_id: str, title: str) -> None:
    try:
        candidates = await client.search_by_paper_title(title)
    except RuntimeError as exc:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="github",
            surface="title_search",
            urls=(),
            evidence_text=title,
            raw_cache_id=None,
            empty_status="fetch_failed",
            error_message=str(exc),
        )
        return

    evidence_text = json.dumps(
        [
            {
                "url": candidate.normalized_repo_url,
                "stars": candidate.stars,
                "created_at": candidate.created_at,
                "description": candidate.description,
            }
            for candidate in candidates[:10]
        ],
        ensure_ascii=False,
    )
    if not candidates:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="github",
            surface="title_search",
            urls=(),
            evidence_text=title,
            raw_cache_id=None,
        )
        return
    if len(candidates) == 1:
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="github",
            surface="title_search",
            urls=(candidates[0].normalized_repo_url,),
            evidence_text=evidence_text,
            raw_cache_id=None,
        )
        return

    database.replace_repo_observations(
        arxiv_id=arxiv_id,
        provider="github",
        surface="title_search",
        observations=[
            {
                "status": "ambiguous",
                "observed_repo_url": candidate.normalized_repo_url,
                "normalized_repo_url": candidate.normalized_repo_url,
                "evidence_text": evidence_text,
                "raw_cache_id": None,
                "extractor_version": EXTRACTOR_VERSION,
            }
            for candidate in candidates[:10]
        ],
    )


async def _run_enrich_repos(
    database: Database,
    github: GitHubClient,
    categories: tuple[str, ...],
    *,
    window: ArxivSyncWindow = ArxivSyncWindow(),
) -> None:
    papers = _list_papers_for_window(database, categories, window=window)
    seen: set[str] = set()
    owner_id = _build_sync_owner_id()
    for paper in papers:
        for link in database.list_paper_repo_links(paper.arxiv_id):
            normalized = normalize_github_url(link.normalized_repo_url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            resource_key = f"repo:{normalized}"
            lease = database.try_acquire_resource_lease(
                resource_key,
                owner_id=owner_id,
                lease_token=str(uuid.uuid4()),
                lease_ttl_seconds=RESOURCE_LEASE_TTL_SECONDS,
            )
            if lease is None:
                print(f"{normalized}: skipped (repo enrich held by another process)")
                continue
            stop_heartbeat = asyncio.Event()
            heartbeat_task = asyncio.create_task(_heartbeat_resource_lease(database, lease, stop_heartbeat))
            try:
                metadata, error = await github.fetch_repo_metadata(normalized)
                if error:
                    print(f"{normalized}: {error}")
                    continue
                _ensure_resource_lease(database, lease)
                if metadata is not None:
                    database.upsert_github_repo(metadata)
                    print(f"{normalized}: enriched")
            finally:
                stop_heartbeat.set()
                await heartbeat_task
                database.release_resource_lease(
                    resource_key,
                    owner_id=lease.owner_id,
                    lease_token=lease.lease_token,
                )


def _run_audit_parity(
    database: Database,
    categories: tuple[str, ...],
    *,
    window: ArxivSyncWindow = ArxivSyncWindow(),
) -> None:
    with database.snapshot_reads():
        papers = _list_papers_for_window(database, categories, window=window)
        total = len(papers)
        found_provider = 0
        found_final = 0
        ambiguous = 0
        for paper in papers:
            observations = database.list_repo_observations(paper.arxiv_id)
            final_links = database.list_paper_repo_links(paper.arxiv_id)
            summary = parity_summary(observations, final_links)
            if summary["found_any_provider_link"]:
                found_provider += 1
            if summary["final_status"] == "found":
                found_final += 1
            elif summary["final_status"] == "ambiguous":
                ambiguous += 1
    print(json.dumps({
        "papers": total,
        "provider_visible_link_papers": found_provider,
        "final_found_papers": found_final,
        "ambiguous_papers": ambiguous,
    }, ensure_ascii=False, indent=2))


def _run_export_csv(
    database: Database,
    categories: tuple[str, ...],
    output_path: Path,
    *,
    window: ArxivSyncWindow = ArxivSyncWindow(),
) -> None:
    with database.snapshot_reads():
        papers = _list_papers_for_window(database, categories, window=window)
        rows: list[dict[str, object]] = []
        for paper in papers:
            links = database.list_paper_repo_links(paper.arxiv_id)
            repo_metadata_by_url = {
                link.normalized_repo_url: metadata
                for link in links
                if (metadata := database.get_github_repo(link.normalized_repo_url)) is not None
            }
            rows.append(build_export_row(paper, links, repo_metadata_by_url))
    resolved_output_path = write_papers_csv(rows, output_path)
    print(str(resolved_output_path))


def _try_reuse_exact_surface(
    database: Database,
    raw_cache: RawCacheStore,
    *,
    arxiv_id: str,
    provider: str,
    surface: str,
    extract_urls: Callable[[str | None], tuple[str, ...]],
    lease: PaperSyncLease | None = None,
) -> tuple[bool, bool, int | None]:
    observations = database.list_surface_repo_observations(arxiv_id, provider, surface)
    if not observations:
        return False, False, None
    latest = observations[-1]
    if latest.status == "fetch_failed":
        return False, False, None
    if latest.status == "checked_no_match" and _is_exact_no_match_expired(latest.observed_at):
        return False, False, None
    raw_cache_id = next((item.raw_cache_id for item in observations if item.raw_cache_id is not None), None)
    if raw_cache_id is None:
        return latest.status == "checked_no_match", False, None
    entry = database.get_raw_cache_by_id(raw_cache_id)
    if entry is None:
        return latest.status == "checked_no_match", False, None
    body = raw_cache.read_body(entry)
    if body is None:
        return latest.status == "checked_no_match", False, entry.status_code
    urls = extract_urls(body)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider=provider,
        surface=surface,
        urls=urls,
        evidence_text=body,
        raw_cache_id=entry.id,
        lease=lease,
    )
    return True, bool(urls), entry.status_code


def _is_exact_no_match_expired(observed_at: str) -> bool:
    try:
        observed = datetime.fromisoformat(observed_at)
    except ValueError:
        return True
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - observed >= EXACT_NO_MATCH_TTL


def _persist_raw_response(
    database: Database,
    raw_cache: RawCacheStore,
    *,
    provider: str,
    surface: str,
    request_key: str,
    request_url: str,
    status: int | None,
    headers: dict[str, str],
    body: str | None,
) -> int | None:
    if status is None or body is None:
        return None
    path, content_hash = raw_cache.write_body(
        provider=provider,
        surface=surface,
        request_key=request_key,
        body=body,
        content_type=headers.get("Content-Type"),
    )
    return database.upsert_raw_cache(
        provider=provider,
        surface=surface,
        request_key=request_key,
        request_url=request_url,
        content_type=headers.get("Content-Type"),
        status_code=status,
        body_path=path,
        content_hash=content_hash,
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    ).id


def _replace_surface_observations(
    database: Database,
    *,
    arxiv_id: str,
    provider: str,
    surface: str,
    urls: tuple[str, ...],
    evidence_text: str | None,
    raw_cache_id: int | None,
    empty_status: str = "checked_no_match",
    error_message: str | None = None,
    lease: PaperSyncLease | None = None,
) -> None:
    database.replace_repo_observations(
        arxiv_id=arxiv_id,
        provider=provider,
        surface=surface,
        observations=[
            {
                "status": "found",
                "observed_repo_url": url,
                "normalized_repo_url": url,
                "evidence_text": evidence_text,
                "raw_cache_id": raw_cache_id,
                "extractor_version": EXTRACTOR_VERSION,
            }
            for url in urls
        ]
        or [
            {
                "status": empty_status,
                "observed_repo_url": None,
                "normalized_repo_url": None,
                "evidence_text": evidence_text,
                "raw_cache_id": raw_cache_id,
                "extractor_version": EXTRACTOR_VERSION,
                "error_message": error_message,
            }
        ],
        lease_owner_id=lease.owner_id if lease is not None else None,
        lease_token=lease.lease_token if lease is not None else None,
    )


def _has_found_repo(observations) -> bool:
    return any(observation.status == "found" and observation.normalized_repo_url for observation in observations)


def _build_sync_owner_id() -> str:
    return f"pid-{os.getpid()}-{uuid.uuid4().hex}"


def _ensure_resource_lease(database: Database, lease) -> None:
    if lease is None:
        return
    if not database.validate_resource_lease(
        lease.resource_key,
        owner_id=lease.owner_id,
        lease_token=lease.lease_token,
    ):
        raise LeaseLostError(f"{lease.resource_key}: resource lease lost")


def _ensure_paper_lease(database: Database, lease: PaperSyncLease | None) -> None:
    if lease is None:
        return
    if not database.validate_paper_sync_lease(
        lease.arxiv_id,
        owner_id=lease.owner_id,
        lease_token=lease.lease_token,
    ):
        raise LeaseLostError(f"{lease.arxiv_id}: paper lease lost")


async def _heartbeat_paper_sync_lease(database: Database, lease: PaperSyncLease, stop_event: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PAPER_SYNC_LEASE_HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            renewed = database.renew_paper_sync_lease(
                lease.arxiv_id,
                owner_id=lease.owner_id,
                lease_token=lease.lease_token,
                lease_ttl_seconds=PAPER_SYNC_LEASE_TTL_SECONDS,
            )
            if not renewed:
                return


async def _heartbeat_resource_lease(database: Database, lease, stop_event: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=RESOURCE_LEASE_HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            renewed = database.renew_resource_lease(
                lease.resource_key,
                owner_id=lease.owner_id,
                lease_token=lease.lease_token,
                lease_ttl_seconds=RESOURCE_LEASE_TTL_SECONDS,
            )
            if not renewed:
                return
