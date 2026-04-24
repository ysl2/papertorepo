from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import session_scope
from papertorepo.db.models import (
    SyncPapersArxivArchiveAppearance,
    SyncPapersArxivDay,
    SyncPapersArxivRequestCheckpoint,
    GitHubRepo,
    JobAttemptMode,
    Paper,
    PaperRepoState,
    RawFetch,
    RepoStableStatus,
    utc_now,
)
import papertorepo.services.pipeline as pipeline
from papertorepo.services.pipeline import backfill_sync_papers_arxiv_archive_appearances, get_dashboard_stats, run_sync_papers, scoped_repos


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _listing_html(arxiv_ids: list[str]) -> str:
    body = "".join(f'<dt><a href="/abs/{arxiv_id}" title="Abstract">arXiv:{arxiv_id}</a></dt>' for arxiv_id in arxiv_ids)
    return f"<html><body><dl>{body}</dl></body></html>"


def _feed_xml(entries: list[tuple[str, str, str]]) -> str:
    rendered = "".join(
        f"""
        <entry>
          <id>http://arxiv.org/abs/{arxiv_id}v1</id>
          <updated>{published_at}T00:00:00Z</updated>
          <published>{published_at}T00:00:00Z</published>
          <title>{title}</title>
          <summary>{title} abstract</summary>
          <author><name>Alice</name></author>
          <category term="cs.CV" scheme="http://arxiv.org/schemas/atom"/>
        </entry>
        """
        for arxiv_id, published_at, title in entries
    )
    return (
        "<?xml version='1.0' encoding='UTF-8'?>"
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:arxiv="http://arxiv.org/schemas/atom">'
        f"{rendered}</feed>"
    )


def _insert_scoped_paper(
    arxiv_id: str,
    published_at: date,
    *,
    categories: list[str] | None = None,
    primary_category: str = "cs.CV",
) -> None:
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                title=f"Paper {arxiv_id}",
                abstract="Example abstract",
                published_at=at_utc_midnight(published_at),
                updated_at=at_utc_midnight(published_at),
                authors_json=["Alice"],
                categories_json=categories or [primary_category],
                comment=None,
                primary_category=primary_category,
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )


def _insert_archive_appearance(*, arxiv_id: str, category: str, archive_month: date) -> None:
    with session_scope() as db:
        db.add(
            SyncPapersArxivArchiveAppearance(
                arxiv_id=arxiv_id,
                category=category,
                archive_month=archive_month,
            )
        )


def _insert_arxiv_sync_day(*, category: str, sync_day: date, last_completed_at: datetime) -> None:
    with session_scope() as db:
        db.add(
            SyncPapersArxivDay(
                category=category,
                sync_day=sync_day,
                last_completed_at=last_completed_at,
            )
        )


@pytest.mark.anyio
async def test_run_sync_papers_reuses_worker_arxiv_rate_limiter_across_runs(db_env, monkeypatch):
    monkeypatch.setenv("SYNC_PAPERS_ARXIV_MIN_INTERVAL", "3.0")
    clear_settings_cache()
    pipeline._SYNC_PAPERS_ARXIV_RATE_LIMITERS.clear()
    rate_limiters: list[object] = []

    class FakeClient:
        def __init__(self, *_args, **kwargs):
            rate_limiters.append(kwargs["rate_limiter"])

        async def fetch_listing_page(self, **_kwargs):
            return 200, _listing_html([]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("empty listing should not hydrate metadata")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        await run_sync_papers(db, {"categories": ["cs.CV"], "month": "2025-03", "force": True})
        await run_sync_papers(db, {"categories": ["cs.CV"], "month": "2025-04", "force": True})

    assert len(rate_limiters) == 2
    assert rate_limiters[0] is rate_limiters[1]


@pytest.mark.anyio
async def test_run_sync_papers_uses_distinct_arxiv_rate_limiter_when_interval_changes(db_env, monkeypatch):
    pipeline._SYNC_PAPERS_ARXIV_RATE_LIMITERS.clear()
    rate_limiters: list[object] = []

    class FakeClient:
        def __init__(self, *_args, **kwargs):
            rate_limiters.append(kwargs["rate_limiter"])

        async def fetch_listing_page(self, **_kwargs):
            return 200, _listing_html([]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("empty listing should not hydrate metadata")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        monkeypatch.setenv("SYNC_PAPERS_ARXIV_MIN_INTERVAL", "3.0")
        clear_settings_cache()
        await run_sync_papers(db, {"categories": ["cs.CV"], "month": "2025-03", "force": True})

        monkeypatch.setenv("SYNC_PAPERS_ARXIV_MIN_INTERVAL", "6.0")
        clear_settings_cache()
        await run_sync_papers(db, {"categories": ["cs.CV"], "month": "2025-04", "force": True})

    assert len(rate_limiters) == 2
    assert rate_limiters[0] is not rate_limiters[1]


@pytest.mark.anyio
async def test_run_sync_papers_window_uses_listing_pages_and_keeps_archive_results(db_env, monkeypatch):
    clients: list[object] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            self.list_calls: list[tuple[str, str, int, int]] = []
            self.id_batch_calls: list[tuple[str, ...]] = []
            self.category_calls: list[tuple[str, int, int]] = []
            clients.append(self)

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            self.list_calls.append((category, period, skip, show))
            if period == "2025-03" and skip == 0:
                return 200, _listing_html(["2503.00001", "2503.00002"]), {"Content-Type": "text/html"}, None
            if period == "2025-04" and skip == 0:
                return 200, _listing_html(["2504.00001"]), {"Content-Type": "text/html"}, None
            return 200, _listing_html([]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, arxiv_ids):
            self.id_batch_calls.append(tuple(arxiv_ids))
            payload = {
                "2503.00001": ("2503.00001", "2025-03-14", "Too early"),
                "2503.00002": ("2503.00002", "2025-03-16", "In range March"),
                "2504.00001": ("2504.00001", "2025-04-10", "In range April"),
            }
            return 200, _feed_xml([payload[item] for item in arxiv_ids]), {"Content-Type": "application/atom+xml"}, None

        async def fetch_category_page(self, *, category, start=0, max_results=100):
            self.category_calls.append((category, start, max_results))
            raise AssertionError("window sync should not fall back to category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {
                "categories": ["cs.CV"],
                "from": "2025-03-15",
                "to": "2025-04-10",
            },
        )

    assert stats["listing_pages_fetched"] == 2
    assert stats["metadata_batches_fetched"] == 2
    assert stats["papers_upserted"] == 3
    assert len(clients) == 1
    assert clients[0].category_calls == []
    assert clients[0].list_calls == [
        ("cs.CV", "2025-03", 0, 2000),
        ("cs.CV", "2025-04", 0, 2000),
    ]

    with session_scope() as db:
        papers = db.query(Paper).order_by(Paper.arxiv_id.asc()).all()
        appearances = (
            db.query(SyncPapersArxivArchiveAppearance)
            .order_by(SyncPapersArxivArchiveAppearance.archive_month.asc(), SyncPapersArxivArchiveAppearance.arxiv_id.asc())
            .all()
        )
        assert [paper.arxiv_id for paper in papers] == ["2503.00001", "2503.00002", "2504.00001"]
        assert [(item.arxiv_id, item.archive_month.isoformat()) for item in appearances] == [
            ("2503.00001", "2025-03-01"),
            ("2503.00002", "2025-03-01"),
            ("2504.00001", "2025-04-01"),
        ]


@pytest.mark.anyio
async def test_run_sync_papers_repair_reuses_arxiv_request_checkpoints(db_env, monkeypatch):
    monkeypatch.setenv("SYNC_PAPERS_ARXIV_ID_BATCH_SIZE", "1")
    clear_settings_cache()
    phase = "fresh"
    list_calls: list[tuple[str, str, str, int, int]] = []
    id_calls: list[tuple[str, tuple[str, ...]]] = []
    scope = {"categories": ["cs.CV"], "month": "2025-04", "force": True}
    payload = {
        "2504.00001": ("2504.00001", "2025-04-01", "First"),
        "2504.00002": ("2504.00002", "2025-04-02", "Second"),
        "2504.00003": ("2504.00003", "2025-04-03", "Third"),
    }

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            list_calls.append((phase, category, period, skip, show))
            return 200, _listing_html(list(payload)), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, arxiv_ids):
            batch = tuple(arxiv_ids)
            id_calls.append((phase, batch))
            if phase == "fresh" and batch == ("2504.00003",):
                return 429, "rate limited", {}, "arXiv metadata id_list query error (429)"
            return 200, _feed_xml([payload[item] for item in arxiv_ids]), {"Content-Type": "application/atom+xml"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        with pytest.raises(RuntimeError, match="429"):
            await run_sync_papers(
                db,
                scope,
                attempt_series_key="series-checkpoint",
                attempt_mode=JobAttemptMode.fresh,
            )

        checkpoints = db.query(SyncPapersArxivRequestCheckpoint).order_by(SyncPapersArxivRequestCheckpoint.surface.asc()).all()
        assert len(checkpoints) == 3
        assert sorted(checkpoint.surface for checkpoint in checkpoints) == ["id_list_feed", "id_list_feed", "listing_html"]

    phase = "repair"
    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            scope,
            attempt_series_key="series-checkpoint",
            attempt_mode=JobAttemptMode.repair,
        )

    assert list_calls == [("fresh", "cs.CV", "2025-04", 0, 2000)]
    assert id_calls == [
        ("fresh", ("2504.00001",)),
        ("fresh", ("2504.00002",)),
        ("fresh", ("2504.00003",)),
        ("repair", ("2504.00003",)),
    ]
    assert stats["checkpoint_reused"] == 3
    assert stats["checkpoint_pages_reused"] == 1
    assert stats["checkpoint_metadata_batches_reused"] == 2
    assert stats["listing_pages_fetched"] == 0
    assert stats["metadata_batches_fetched"] == 1

    with session_scope() as db:
        papers = db.query(Paper).order_by(Paper.arxiv_id.asc()).all()
        assert [paper.arxiv_id for paper in papers] == ["2504.00001", "2504.00002", "2504.00003"]
        days = db.query(SyncPapersArxivDay).filter(SyncPapersArxivDay.category == "cs.CV").all()
        assert len(days) == 30


@pytest.mark.anyio
async def test_run_sync_papers_fresh_attempt_ignores_previous_checkpoints(db_env, monkeypatch):
    monkeypatch.setenv("SYNC_PAPERS_ARXIV_ID_BATCH_SIZE", "1")
    clear_settings_cache()
    calls: list[tuple[str, tuple[str, ...] | None]] = []
    phase = "first"
    scope = {"categories": ["cs.CV"], "month": "2025-04", "force": True}

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            calls.append((phase, None))
            return 200, _listing_html(["2504.00001"]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, arxiv_ids):
            calls.append((phase, tuple(arxiv_ids)))
            return 200, _feed_xml([("2504.00001", "2025-04-01", phase)]), {"Content-Type": "application/atom+xml"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        await run_sync_papers(
            db,
            scope,
            attempt_series_key="first-series",
            attempt_mode=JobAttemptMode.fresh,
        )

    phase = "second"
    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            scope,
            attempt_series_key="second-series",
            attempt_mode=JobAttemptMode.fresh,
        )

    assert calls == [
        ("first", None),
        ("first", ("2504.00001",)),
        ("second", None),
        ("second", ("2504.00001",)),
    ]
    assert stats["checkpoint_reused"] == 0


@pytest.mark.anyio
async def test_run_sync_papers_skips_range_month_when_requested_days_are_fresh(db_env, monkeypatch):
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    for sync_day in [date(2025, 3, day) for day in range(15, 32)]:
        _insert_arxiv_sync_day(category="cs.CV", sync_day=sync_day, last_completed_at=now - timedelta(days=10))

    class FailClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, **_kwargs):
            raise AssertionError("fresh closed-window TTL should skip arXiv listing fetches")

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("fresh closed-window TTL should skip arXiv metadata batch fetches")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("closed-window TTL should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FailClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {
                "categories": ["cs.CV"],
                "from": "2025-03-15",
                "to": "2025-03-31",
                "force": False,
            },
        )

    assert stats["windows_skipped_ttl"] == 1
    assert stats["pages_fetched"] == 0
    assert stats["listing_pages_fetched"] == 0
    assert stats["metadata_batches_fetched"] == 0


@pytest.mark.anyio
async def test_run_sync_papers_force_bypasses_daily_ttl(db_env, monkeypatch):
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    for sync_day in [date(2025, 3, day) for day in range(1, 32)]:
        _insert_arxiv_sync_day(category="cs.CV", sync_day=sync_day, last_completed_at=now - timedelta(days=10))
    clients: list[object] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            self.list_calls: list[tuple[str, str, int, int]] = []
            clients.append(self)

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            self.list_calls.append((category, period, skip, show))
            return 200, _listing_html([]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("empty listing should not trigger metadata batch fetches")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("window sync should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {
                "categories": ["cs.CV"],
                "from": "2025-03-01",
                "to": "2025-03-31",
                "force": True,
            },
        )

    assert stats["windows_skipped_ttl"] == 0
    assert stats["listing_pages_fetched"] == 1
    assert len(clients) == 1
    assert clients[0].list_calls == [("cs.CV", "2025-03", 0, 2000)]


@pytest.mark.anyio
async def test_run_sync_papers_records_closed_window_completion(db_env, monkeypatch):
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            assert (category, period, skip, show) == ("cs.CV", "2025-03", 0, 2000)
            return 200, _listing_html([]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("empty listing should not trigger metadata batch fetches")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("window sync should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {
                "categories": ["cs.CV"],
                "from": "2025-03-01",
                "to": "2025-03-31",
                "force": False,
            },
        )

    assert stats["windows_skipped_ttl"] == 0
    assert stats["listing_pages_fetched"] == 1

    with session_scope() as db:
        rows = db.query(SyncPapersArxivDay).filter(SyncPapersArxivDay.category == "cs.CV").order_by(SyncPapersArxivDay.sync_day.asc()).all()

    assert [row.sync_day for row in rows] == [date(2025, 3, day) for day in range(1, 32)]
    for row in rows:
        assert row.last_completed_at is not None
        completed_at = row.last_completed_at
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)
        assert completed_at == now


@pytest.mark.anyio
async def test_run_sync_papers_range_checks_only_requested_days_within_month(db_env, monkeypatch):
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    for sync_day in [date(2026, 4, day) for day in range(1, 11)]:
        _insert_arxiv_sync_day(category="cs.CV", sync_day=sync_day, last_completed_at=now)

    class FailClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, **_kwargs):
            raise AssertionError("fresh requested days should skip arXiv listing fetches")

        async def fetch_id_list_feed(self, _arxiv_ids):
            raise AssertionError("fresh requested days should skip metadata fetches")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("range sync should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FailClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {
                "categories": ["cs.CV"],
                "from": "2026-04-01",
                "to": "2026-04-10",
                "force": False,
            },
        )

    assert stats["windows_skipped_ttl"] == 1
    assert stats["listing_pages_fetched"] == 0


@pytest.mark.anyio
async def test_run_sync_papers_recent_day_uses_catchup(db_env, monkeypatch):
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_catchup_page(self, *, category, day):
            calls.append(("catchup", day.isoformat()))
            return 200, _listing_html(["2604.00001"]), {"Content-Type": "text/html"}, None

        async def fetch_id_list_feed(self, arxiv_ids):
            assert arxiv_ids == ["2604.00001"]
            return 200, _feed_xml([("2604.00001", "2026-04-20", "Recent day")]), {"Content-Type": "application/atom+xml"}, None

        async def fetch_listing_page(self, **_kwargs):
            raise AssertionError("recent day should not use month listing")

        async def fetch_submitted_day_page(self, **_kwargs):
            raise AssertionError("recent day should not use submittedDate fallback")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("day sync should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(db, {"categories": ["cs.CV"], "day": "2026-04-20"})

    assert stats["catchup_pages_fetched"] == 1
    assert stats["search_pages_fetched"] == 0
    assert calls == [("catchup", "2026-04-20")]


@pytest.mark.anyio
async def test_run_sync_papers_historical_day_uses_submitted_date_fallback(db_env, monkeypatch):
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_submitted_day_page(self, *, category, day, start=0, max_results=2000):
            calls.append(("submitted", day.isoformat()))
            assert start == 0
            return 200, _feed_xml([("2501.00001", "2025-01-10", "Historical day")]), {"Content-Type": "application/atom+xml"}, None

        async def fetch_id_list_feed(self, arxiv_ids):
            assert arxiv_ids == ["2501.00001"]
            return 200, _feed_xml([("2501.00001", "2025-01-10", "Historical day")]), {"Content-Type": "application/atom+xml"}, None

        async def fetch_catchup_page(self, **_kwargs):
            raise AssertionError("historical day should not use catchup")

        async def fetch_listing_page(self, **_kwargs):
            raise AssertionError("historical day should not use month listing")

        async def fetch_category_page(self, **_kwargs):
            raise AssertionError("day sync should not use category-page fetching")

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)
    monkeypatch.setattr("papertorepo.services.pipeline._now_utc", lambda: now)
    monkeypatch.setattr("papertorepo.services.pipeline._today_utc", lambda: now.date())

    with session_scope() as db:
        stats = await run_sync_papers(db, {"categories": ["cs.CV"], "day": "2025-01-10"})

    assert stats["search_pages_fetched"] == 1
    assert stats["catchup_pages_fetched"] == 0
    assert calls == [("submitted", "2025-01-10")]


def test_dashboard_stats_and_scoped_repos_track_refreshed_metadata_scope(db_env):
    _insert_scoped_paper("2504.00001", date(2025, 4, 10), categories=["cs.AI", "cs.CV"], primary_category="cs.AI")
    _insert_scoped_paper("2504.00002", date(2025, 4, 11))
    _insert_scoped_paper("2505.00003", date(2025, 5, 2))
    _insert_scoped_paper("2601.00001", date(2026, 1, 3))
    _insert_archive_appearance(arxiv_id="2504.00001", category="cs.CV", archive_month=date(2025, 4, 1))
    _insert_archive_appearance(arxiv_id="2505.00003", category="cs.CV", archive_month=date(2025, 4, 1))
    _insert_archive_appearance(arxiv_id="2601.00001", category="cs.CV", archive_month=date(2026, 1, 1))

    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2504.00001",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/in-scope",
                repo_urls_json=["https://github.com/foo/in-scope"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2504.00002",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/not-refreshed",
                repo_urls_json=["https://github.com/foo/not-refreshed"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2601.00001",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/out-of-scope",
                repo_urls_json=["https://github.com/foo/out-of-scope"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2505.00003",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/archive-only",
                repo_urls_json=["https://github.com/foo/archive-only"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/in-scope",
                owner="foo",
                repo="in-scope",
                first_seen_at=utc_now(),
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/out-of-scope",
                owner="foo",
                repo="out-of-scope",
                first_seen_at=utc_now(),
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/archive-only",
                owner="foo",
                repo="archive-only",
                first_seen_at=utc_now(),
            )
        )

    scope = {"categories": ["cs.CV"], "from": "2025-04-01", "to": "2025-04-30"}
    with session_scope() as db:
        stats = get_dashboard_stats(db, scope)
        repos = scoped_repos(db, scope)

    assert stats["papers"] == 2
    assert stats["found"] == 2
    assert stats["unknown"] == 0
    assert stats["repos"] == 1
    assert [repo.normalized_github_url for repo in repos] == ["https://github.com/foo/in-scope"]


def test_dashboard_stats_count_unknown_from_missing_and_unknown_repo_states(db_env):
    _insert_scoped_paper("2504.10001", date(2025, 4, 10))
    _insert_scoped_paper("2504.10002", date(2025, 4, 10))
    _insert_scoped_paper("2504.10003", date(2025, 4, 10))
    _insert_scoped_paper("2504.10004", date(2025, 4, 10))
    _insert_scoped_paper("2504.10005", date(2025, 4, 10))

    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2504.10001",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/found",
                repo_urls_json=["https://github.com/foo/found"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2504.10002",
                stable_status=RepoStableStatus.not_found,
                primary_repo_url=None,
                repo_urls_json=[],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2504.10003",
                stable_status=RepoStableStatus.ambiguous,
                primary_repo_url="https://github.com/foo/ambiguous-a",
                repo_urls_json=[
                    "https://github.com/foo/ambiguous-a",
                    "https://github.com/foo/ambiguous-b",
                ],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2504.10004",
                stable_status=RepoStableStatus.unknown,
                primary_repo_url=None,
                repo_urls_json=[],
                stable_decided_at=None,
                refresh_after=None,
                last_attempt_at=utc_now(),
                last_attempt_complete=False,
            )
        )

    with session_scope() as db:
        stats = get_dashboard_stats(db, {})

    assert stats["papers"] == 5
    assert stats["found"] == 1
    assert stats["not_found"] == 1
    assert stats["ambiguous"] == 1
    assert stats["unknown"] == 2


def test_backfill_sync_papers_arxiv_archive_appearances_uses_stored_listing_html(db_env):
    _insert_scoped_paper("2504.00001", date(2025, 4, 10))
    raw_dir = db_env / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    body_path = raw_dir / "listing.html"
    body_path.write_text(_listing_html(["2504.00001", "2504.99999"]), encoding="utf-8")

    with session_scope() as db:
        db.add(
            RawFetch(
                provider="arxiv",
                surface="listing_html",
                request_key="list:cs.CV:2025-04:0:2000",
                request_url="https://arxiv.org/list/cs.CV/2025-04?skip=0&show=2000",
                status_code=200,
                content_type="text/html",
                headers_json={"Content-Type": "text/html"},
                body_path=str(body_path),
                content_hash="hash",
                fetched_at=utc_now(),
            )
        )

    with session_scope() as db:
        stats = backfill_sync_papers_arxiv_archive_appearances(db)
        appearances = db.query(SyncPapersArxivArchiveAppearance).all()

    assert stats["listing_fetches"] == 1
    assert stats["appearances_created"] == 1
    assert [(item.arxiv_id, item.category, item.archive_month.isoformat()) for item in appearances] == [
        ("2504.00001", "cs.CV", "2025-04-01")
    ]
