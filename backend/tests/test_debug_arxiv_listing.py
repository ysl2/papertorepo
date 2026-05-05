from __future__ import annotations

import csv
from datetime import date, datetime, timezone

import pytest

from papertorepo.services.debug_arxiv_listing import BASELINE_FIELDNAMES, compare_listing_baseline_against_db, generate_listing_baseline
from papertorepo.db.session import session_scope
from papertorepo.db.models import SyncPapersArxivArchiveAppearance, Paper, utc_now


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _listing_html(arxiv_ids: list[str]) -> str:
    body = "".join(f'<dt><a href="/abs/{arxiv_id}" title="Abstract">arXiv:{arxiv_id}</a></dt>' for arxiv_id in arxiv_ids)
    return f"<html><body><dl>{body}</dl></body></html>"


@pytest.mark.anyio
async def test_generate_listing_baseline_paginates_and_writes_month_csv(tmp_path, monkeypatch):
    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            self.calls: list[tuple[str, str, int, int]] = []

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            self.calls.append((category, period, skip, show))
            pages = {
                ("cs.CV", "2025-03", 0, 2): _listing_html(["2503.00001", "2503.00002"]),
                ("cs.CV", "2025-03", 2, 2): _listing_html(["2503.00003"]),
            }
            return 200, pages.get((category, period, skip, show), _listing_html([])), {"Content-Type": "text/html"}, None

    monkeypatch.setattr("papertorepo.services.debug_arxiv_listing.ArxivMetadataClient", FakeClient)

    result = await generate_listing_baseline(
        {
            "categories": ["cs.CV"],
            "month": "2025-03",
        },
        output_root=tmp_path / "baseline",
        page_size=2,
    )

    assert result["months_generated"] == 1
    assert result["rows_generated"] == 3
    month_csv = tmp_path / "baseline" / "cs.CV" / "2025-03.csv"
    with month_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["arxiv_id"] == "2503.00001"
    assert rows[1]["arxiv_id"] == "2503.00002"
    assert rows[2]["arxiv_id"] == "2503.00003"
    assert rows[0]["page_skip"] == "0"
    assert rows[2]["page_skip"] == "2"
    assert rows[2]["global_position"] == "3"
    assert rows[2]["source_url"].endswith("/list/cs.CV/2025-03?skip=2&show=2")


def test_compare_listing_baseline_against_db_reports_missing_and_extra(db_env, tmp_path):
    baseline_root = tmp_path / "baseline"
    month_dir = baseline_root / "cs.CV"
    month_dir.mkdir(parents=True, exist_ok=True)
    month_csv = month_dir / "2025-03.csv"

    with month_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=BASELINE_FIELDNAMES)
        writer.writeheader()
        for index, arxiv_id in enumerate(["2503.00001", "2503.00002", "2503.00003"], start=1):
            writer.writerow(
                {
                    "category": "cs.CV",
                    "archive_month": "2025-03",
                    "page_skip": 0,
                    "page_size": 2000,
                    "page_index": 0,
                    "position_in_page": index,
                    "global_position": index,
                    "arxiv_id": arxiv_id,
                    "abs_url": f"https://arxiv.org/abs/{arxiv_id}",
                    "source_url": "https://arxiv.org/list/cs.CV/2025-03?skip=0&show=2000",
                }
            )

    with session_scope() as db:
        db.add_all(
            [
                Paper(
                    arxiv_id="2503.00001",
                    abs_url="https://arxiv.org/abs/2503.00001",
                    title="Paper 1",
                    abstract="Example",
                    published_at=at_utc_midnight(date(2025, 3, 1)),
                    updated_at=at_utc_midnight(date(2025, 3, 1)),
                    authors_json=["Alice"],
                    categories_json=["cs.CV"],
                    comment=None,
                    primary_category="cs.CV",
                    source_first_seen_at=utc_now(),
                    source_last_seen_at=utc_now(),
                ),
                Paper(
                    arxiv_id="2503.00002",
                    abs_url="https://arxiv.org/abs/2503.00002",
                    title="Paper 2",
                    abstract="Example",
                    published_at=at_utc_midnight(date(2025, 3, 2)),
                    updated_at=at_utc_midnight(date(2025, 3, 2)),
                    authors_json=["Bob"],
                    categories_json=["cs.CV"],
                    comment=None,
                    primary_category="cs.CV",
                    source_first_seen_at=utc_now(),
                    source_last_seen_at=utc_now(),
                ),
                SyncPapersArxivArchiveAppearance(arxiv_id="2503.00001", category="cs.CV", archive_month=date(2025, 3, 1)),
                SyncPapersArxivArchiveAppearance(arxiv_id="2503.00002", category="cs.CV", archive_month=date(2025, 3, 1)),
            ]
        )

    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2503.99999",
                abs_url="https://arxiv.org/abs/2503.99999",
                title="Paper extra",
                abstract="Example",
                published_at=at_utc_midnight(date(2025, 3, 3)),
                updated_at=at_utc_midnight(date(2025, 3, 3)),
                authors_json=["Carol"],
                categories_json=["cs.CV"],
                comment=None,
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(SyncPapersArxivArchiveAppearance(arxiv_id="2503.99999", category="cs.CV", archive_month=date(2025, 3, 1)))

    with session_scope() as db:
        result = compare_listing_baseline_against_db(
            db,
            {
                "categories": ["cs.CV"],
                "month": "2025-03",
            },
            baseline_root=baseline_root,
            compare_root=tmp_path / "compare",
        )

    assert result["months_compared"] == 1
    assert result["total_missing_archive_appearances"] == 1
    assert result["total_missing_papers"] == 1
    assert result["total_extra_archive_appearances"] == 1

    summary_csv = tmp_path / "compare" / "compare-summary.csv"
    with summary_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["missing_archive_appearance_count"] == "1"
    assert rows[0]["missing_paper_count"] == "1"
    assert rows[0]["extra_archive_appearance_count"] == "1"
