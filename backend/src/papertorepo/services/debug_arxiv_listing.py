from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from sqlalchemy import select
from sqlalchemy.orm import Session

from papertorepo.core.http import build_timeout
from papertorepo.core.normalize.arxiv import build_arxiv_abs_url
from papertorepo.providers.arxiv_metadata import ArxivMetadataClient
from papertorepo.core.config import get_settings
from papertorepo.db.models import SyncPapersArxivArchiveAppearance, Paper
from papertorepo.core.scope import month_label, resolve_archive_months_from_scope_json, resolve_categories_from_scope_json
from papertorepo.services.pipeline import _extract_arxiv_ids_from_listing_html


DEFAULT_LISTING_PAGE_SIZE = 2000
BASELINE_FIELDNAMES = [
    "category",
    "archive_month",
    "page_skip",
    "page_size",
    "page_index",
    "position_in_page",
    "global_position",
    "arxiv_id",
    "abs_url",
    "source_url",
]
BASELINE_SUMMARY_FIELDNAMES = [
    "category",
    "archive_month",
    "row_count",
    "unique_id_count",
    "duplicate_row_count",
    "page_count",
    "first_arxiv_id",
    "last_arxiv_id",
    "output_csv",
    "fetched_at",
]
COMPARE_SUMMARY_FIELDNAMES = [
    "category",
    "archive_month",
    "baseline_row_count",
    "baseline_unique_id_count",
    "baseline_duplicate_row_count",
    "db_archive_appearance_count",
    "db_paper_presence_count",
    "missing_archive_appearance_count",
    "missing_paper_count",
    "extra_archive_appearance_count",
    "missing_archive_appearance_csv",
    "missing_paper_csv",
    "extra_archive_appearance_csv",
]


@dataclass(frozen=True)
class ListingMonthSummary:
    category: str
    archive_month: str
    row_count: int
    unique_id_count: int
    duplicate_row_count: int
    page_count: int
    first_arxiv_id: str
    last_arxiv_id: str
    output_csv: str
    fetched_at: str


def _baseline_root_dir(output_root: Path | None = None) -> Path:
    if output_root is not None:
        return output_root
    settings = get_settings()
    return settings.data_dir / "debug" / "arxiv_listing_baseline"


def _compare_root_dir(compare_root: Path | None = None) -> Path:
    if compare_root is not None:
        return compare_root
    return _baseline_root_dir() / "_compare"


def _ensure_archive_months(scope_json: dict[str, Any]) -> list[date]:
    archive_months = resolve_archive_months_from_scope_json(scope_json)
    if not archive_months:
        raise RuntimeError("debug arXiv listing baseline requires a day, month, or complete from/to range")
    return archive_months


def _ensure_categories(scope_json: dict[str, Any]) -> list[str]:
    categories = resolve_categories_from_scope_json(scope_json)
    if not categories:
        raise RuntimeError("debug arXiv listing baseline requires at least one category")
    return categories


def _listing_source_url(category: str, archive_month: str, *, skip: int, page_size: int) -> str:
    return f"https://arxiv.org/list/{category}/{archive_month}?skip={skip}&show={page_size}"


def _write_rows(path: Path, *, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


async def _fetch_listing_month(
    client: ArxivMetadataClient,
    *,
    category: str,
    archive_month: date,
    output_root: Path,
    page_size: int,
) -> ListingMonthSummary:
    archive_month_label = month_label(archive_month)
    output_csv = output_root / category / f"{archive_month_label}.csv"
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, object]] = []
    skip = 0
    page_index = 0

    while True:
        status, body, _headers, error = await client.fetch_listing_page(
            category=category,
            period=archive_month_label,
            skip=skip,
            show=page_size,
        )
        if error or body is None or status is None:
            raise RuntimeError(f"{category}:{archive_month_label} listing fetch failed ({error or 'empty response'})")
        if status != 200:
            raise RuntimeError(f"{category}:{archive_month_label} listing fetch returned HTTP {status}")

        arxiv_ids = _extract_arxiv_ids_from_listing_html(body)
        if not arxiv_ids:
            break

        source_url = _listing_source_url(category, archive_month_label, skip=skip, page_size=page_size)
        for position_in_page, arxiv_id in enumerate(arxiv_ids, start=1):
            rows.append(
                {
                    "category": category,
                    "archive_month": archive_month_label,
                    "page_skip": skip,
                    "page_size": page_size,
                    "page_index": page_index,
                    "position_in_page": position_in_page,
                    "global_position": len(rows) + 1,
                    "arxiv_id": arxiv_id,
                    "abs_url": build_arxiv_abs_url(arxiv_id),
                    "source_url": source_url,
                }
            )

        page_index += 1
        if len(arxiv_ids) < page_size:
            break
        skip += page_size

    _write_rows(output_csv, fieldnames=BASELINE_FIELDNAMES, rows=rows)
    unique_ids = {str(row["arxiv_id"]) for row in rows}
    return ListingMonthSummary(
        category=category,
        archive_month=archive_month_label,
        row_count=len(rows),
        unique_id_count=len(unique_ids),
        duplicate_row_count=len(rows) - len(unique_ids),
        page_count=page_index,
        first_arxiv_id=str(rows[0]["arxiv_id"]) if rows else "",
        last_arxiv_id=str(rows[-1]["arxiv_id"]) if rows else "",
        output_csv=str(output_csv),
        fetched_at=fetched_at,
    )


async def generate_listing_baseline(
    scope_json: dict[str, Any],
    *,
    output_root: Path | None = None,
    page_size: int = DEFAULT_LISTING_PAGE_SIZE,
) -> dict[str, Any]:
    categories = _ensure_categories(scope_json)
    archive_months = _ensure_archive_months(scope_json)
    root_dir = _baseline_root_dir(output_root)
    summaries: list[ListingMonthSummary] = []

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        client = ArxivMetadataClient(session, min_interval=get_settings().sync_papers_arxiv_min_interval)
        for category in categories:
            for archive_month in archive_months:
                summaries.append(
                    await _fetch_listing_month(
                        client,
                        category=category,
                        archive_month=archive_month,
                        output_root=root_dir,
                        page_size=page_size,
                    )
                )

    summary_rows = [asdict(item) for item in summaries]
    _write_rows(root_dir / "baseline-summary.csv", fieldnames=BASELINE_SUMMARY_FIELDNAMES, rows=summary_rows)
    return {
        "output_root": str(root_dir),
        "categories": categories,
        "archive_months": [month_label(value) for value in archive_months],
        "months_generated": len(summaries),
        "rows_generated": sum(item.row_count for item in summaries),
        "summary_csv": str(root_dir / "baseline-summary.csv"),
    }


def _load_month_baseline(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        raise RuntimeError(f"Baseline CSV not found: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        return [str(row["arxiv_id"]).strip() for row in csv.DictReader(handle) if str(row.get("arxiv_id", "")).strip()]


def _write_detail_csv(path: Path, *, category: str, archive_month: str, arxiv_ids: list[str]) -> str:
    rows = [
        {
            "category": category,
            "archive_month": archive_month,
            "arxiv_id": arxiv_id,
            "abs_url": build_arxiv_abs_url(arxiv_id),
        }
        for arxiv_id in arxiv_ids
    ]
    _write_rows(path, fieldnames=["category", "archive_month", "arxiv_id", "abs_url"], rows=rows)
    return str(path)


def compare_listing_baseline_against_db(
    db: Session,
    scope_json: dict[str, Any],
    *,
    baseline_root: Path | None = None,
    compare_root: Path | None = None,
) -> dict[str, Any]:
    categories = _ensure_categories(scope_json)
    archive_months = _ensure_archive_months(scope_json)
    resolved_baseline_root = _baseline_root_dir(baseline_root)
    resolved_compare_root = _compare_root_dir(compare_root)
    summary_rows: list[dict[str, object]] = []

    for category in categories:
        for archive_month in archive_months:
            archive_month_label = month_label(archive_month)
            baseline_csv = resolved_baseline_root / category / f"{archive_month_label}.csv"
            baseline_ids = _load_month_baseline(baseline_csv)
            baseline_unique_ids = list(dict.fromkeys(baseline_ids))
            baseline_unique_set = set(baseline_unique_ids)

            appearance_ids = list(
                db.scalars(
                    select(SyncPapersArxivArchiveAppearance.arxiv_id).where(
                        SyncPapersArxivArchiveAppearance.category == category,
                        SyncPapersArxivArchiveAppearance.archive_month == archive_month,
                    )
                ).all()
            )
            appearance_set = set(appearance_ids)
            paper_ids = set(db.scalars(select(Paper.arxiv_id).where(Paper.arxiv_id.in_(baseline_unique_ids))).all())

            missing_appearance_ids = [arxiv_id for arxiv_id in baseline_unique_ids if arxiv_id not in appearance_set]
            missing_paper_ids = [arxiv_id for arxiv_id in baseline_unique_ids if arxiv_id not in paper_ids]
            extra_appearance_ids = sorted(appearance_set - baseline_unique_set)

            category_compare_dir = resolved_compare_root / category
            missing_appearance_csv = category_compare_dir / f"{archive_month_label}-missing-archive-appearance.csv"
            missing_paper_csv = category_compare_dir / f"{archive_month_label}-missing-paper.csv"
            extra_appearance_csv = category_compare_dir / f"{archive_month_label}-extra-archive-appearance.csv"

            summary_rows.append(
                {
                    "category": category,
                    "archive_month": archive_month_label,
                    "baseline_row_count": len(baseline_ids),
                    "baseline_unique_id_count": len(baseline_unique_set),
                    "baseline_duplicate_row_count": len(baseline_ids) - len(baseline_unique_set),
                    "db_archive_appearance_count": len(appearance_set),
                    "db_paper_presence_count": len(paper_ids),
                    "missing_archive_appearance_count": len(missing_appearance_ids),
                    "missing_paper_count": len(missing_paper_ids),
                    "extra_archive_appearance_count": len(extra_appearance_ids),
                    "missing_archive_appearance_csv": _write_detail_csv(
                        missing_appearance_csv,
                        category=category,
                        archive_month=archive_month_label,
                        arxiv_ids=missing_appearance_ids,
                    ),
                    "missing_paper_csv": _write_detail_csv(
                        missing_paper_csv,
                        category=category,
                        archive_month=archive_month_label,
                        arxiv_ids=missing_paper_ids,
                    ),
                    "extra_archive_appearance_csv": _write_detail_csv(
                        extra_appearance_csv,
                        category=category,
                        archive_month=archive_month_label,
                        arxiv_ids=extra_appearance_ids,
                    ),
                }
            )

    summary_csv = resolved_compare_root / "compare-summary.csv"
    _write_rows(summary_csv, fieldnames=COMPARE_SUMMARY_FIELDNAMES, rows=summary_rows)
    return {
        "baseline_root": str(resolved_baseline_root),
        "compare_root": str(resolved_compare_root),
        "categories": categories,
        "archive_months": [month_label(value) for value in archive_months],
        "months_compared": len(summary_rows),
        "summary_csv": str(summary_csv),
        "total_missing_archive_appearances": sum(int(row["missing_archive_appearance_count"]) for row in summary_rows),
        "total_missing_papers": sum(int(row["missing_paper_count"]) for row in summary_rows),
        "total_extra_archive_appearances": sum(int(row["extra_archive_appearance_count"]) for row in summary_rows),
    }
