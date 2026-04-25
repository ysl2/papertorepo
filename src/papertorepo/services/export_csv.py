from __future__ import annotations

import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile

from papertorepo.core.records import GitHubRepoMetadata, Paper, PaperRepoLink


CSV_COLUMNS = [
    "arxiv_id",
    "abs_url",
    "title",
    "abstract",
    "published_at",
    "categories",
    "primary_category",
    "primary_github_url",
    "github_urls",
    "link_status",
    "stargazers_count",
    "created_at",
    "description",
]


def write_papers_csv(rows: list[dict[str, object]], output_path: Path) -> Path:
    final_path = build_timestamped_csv_path(output_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=final_path.parent,
        prefix=f".{final_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    os.replace(temp_path, final_path)
    return final_path


def build_timestamped_csv_path(output_path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    suffix = output_path.suffix if output_path.suffix else ".csv"
    stem = output_path.stem if output_path.suffix else output_path.name
    return output_path.with_name(f"{stem}-{timestamp}{suffix}")


def build_export_row(
    paper: Paper,
    links: list[PaperRepoLink],
    repo_metadata_by_url: dict[str, GitHubRepoMetadata],
) -> dict[str, object]:
    primary_link = next((link for link in links if link.is_primary), None)
    primary_url = primary_link.github_url if primary_link else ""
    metadata = repo_metadata_by_url.get(primary_url) if primary_url else None
    all_links = "; ".join(link.github_url for link in links)
    link_status = primary_link.status if primary_link else "not_found"
    return {
        "arxiv_id": paper.arxiv_id,
        "abs_url": paper.abs_url,
        "title": paper.title,
        "abstract": paper.abstract,
        "published_at": paper.published_at or "",
        "categories": ", ".join(paper.categories),
        "primary_category": paper.primary_category or "",
        "primary_github_url": primary_url,
        "github_urls": all_links,
        "link_status": link_status,
        "stargazers_count": metadata.stargazers_count if metadata else "",
        "created_at": metadata.created_at if metadata else "",
        "description": metadata.description if metadata else "",
    }
