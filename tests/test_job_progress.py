from __future__ import annotations

from datetime import datetime, timezone

import pytest

from papertorepo.jobs.queue import claim_next_job, create_job, process_job
from papertorepo.db.session import session_scope
from papertorepo.db.models import Job, JobStatus, JobType
from papertorepo.api.schemas import ScopePayload
from papertorepo.services.pipeline import run_sync_papers


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


@pytest.mark.anyio
async def test_run_sync_papers_reports_progress_snapshots(db_env, monkeypatch):
    snapshots: list[dict[str, int]] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            assert category == "cs.CV"
            assert period == "2026-04"
            assert skip == 0
            assert show == 2000
            return (
                200,
                '<html><body><a href="/abs/2604.12345">arXiv:2604.12345</a></body></html>',
                {"Content-Type": "text/html"},
                None,
            )

        async def fetch_id_list_feed(self, arxiv_ids):
            assert arxiv_ids == ["2604.12345"]
            return (
                200,
                _feed_xml([("2604.12345", "2026-04-18", "Example paper")]),
                {"Content-Type": "application/atom+xml"},
                None,
            )

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            progress=lambda current: snapshots.append(dict(current)),
        )

    assert snapshots[0]["categories"] == 1
    assert snapshots[0]["papers_upserted"] == 0
    assert snapshots[0]["pages_fetched"] == 0
    assert snapshots[0]["listing_pages_fetched"] == 0
    assert snapshots[0]["metadata_batches_fetched"] == 0
    assert snapshots[0]["categories_skipped_locked"] == 0
    assert snapshots[0]["windows_skipped_ttl"] == 0
    assert snapshots[-1]["pages_fetched"] == 2
    assert snapshots[-1]["papers_upserted"] == 1
    assert stats["pages_fetched"] == 2
    assert stats["papers_upserted"] == 1


@pytest.mark.anyio
async def test_process_job_failure_keeps_partial_stats(db_env, monkeypatch):
    with session_scope() as db:
        job = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-04"))

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")

    assert claimed is not None
    assert claimed.id == job.id

    async def fake_run_sync_papers(_db, _scope_json, *, progress=None, stop_check=None):
        assert progress is not None
        _ = stop_check
        progress({"categories": 1, "pages_fetched": 3})
        raise RuntimeError("boom")

    monkeypatch.setattr("papertorepo.jobs.queue.run_sync_papers", fake_run_sync_papers)

    await process_job(job.id)

    with session_scope() as db:
        refreshed = db.get(Job, job.id)
        assert refreshed is not None
        assert refreshed.status == JobStatus.failed
        assert refreshed.stats_json == {"categories": 1, "pages_fetched": 3}
        assert refreshed.error_text == "boom"
        assert refreshed.locked_at is not None


def test_claim_next_job_prefers_older_scope_when_created_at_matches(db_env):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        april = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-04"))
        may = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-05"))
        april.created_at = same_created_at
        may.created_at = same_created_at
        db.add_all([april, may])

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")

    assert claimed is not None
    assert claimed.id == april.id
