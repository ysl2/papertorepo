from __future__ import annotations

"""Historical December 2025 reproduction script.

This script is not the canonical `main.py` workflow. It preserves an older
experimental path that can fall back to title-search surfaces, including
Hugging Face and GitHub title search, while the supported CLI remains
exact-match only.
"""

import asyncio
import json
import sys
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ghstars.associate.resolver import build_final_links, parity_summary
from src.ghstars.cli import _run_enrich_repos
from src.ghstars.config import load_config
from src.ghstars.export.csv import build_export_row, write_papers_csv
from src.ghstars.net.http import build_timeout
from src.ghstars.normalize.github import normalize_github_url
from src.ghstars.providers.alphaxiv_links import AlphaXivLinksClient
from src.ghstars.providers.arxiv_links import ArxivLinksClient
from src.ghstars.providers.github import GitHubClient
from src.ghstars.providers.github_search import GitHubRepositorySearchClient
from src.ghstars.providers.huggingface_links import HuggingFaceLinksClient
from src.ghstars.storage.db import Database
from src.ghstars.storage.raw_cache import RawCacheStore


START_DATE = "2025-12-01"
END_DATE = "2025-12-31"
OUTPUT_PATH = Path("output/month-smoke/cs.CV-2025-12.csv")


async def main() -> int:
    config = load_config()
    database = Database(config.db_path)
    raw_cache = RawCacheStore(config.raw_dir)
    try:
        papers = [
            paper
            for paper in database.list_papers_by_categories(("cs.CV",))
            if paper.published_at is not None and START_DATE <= paper.published_at <= END_DATE
        ]
        paper_ids = {paper.arxiv_id for paper in papers}

        async with aiohttp.ClientSession(timeout=build_timeout()) as session:
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
            github_search = GitHubRepositorySearchClient(
                session,
                github_token=config.github_token,
                min_interval=config.github_min_interval,
            )

            for paper in papers:
                await _sync_paper(database, raw_cache, arxiv_links, huggingface, alphaxiv, github_search, paper)
                final_links = build_final_links(paper.arxiv_id, database.list_repo_observations(paper.arxiv_id))
                database.replace_paper_repo_links(paper.arxiv_id, final_links)
                print(f"{paper.arxiv_id}: {len(final_links)} final links")

            seen: set[str] = set()
            for paper in papers:
                for link in database.list_paper_repo_links(paper.arxiv_id):
                    normalized = normalize_github_url(link.normalized_repo_url)
                    if not normalized or normalized in seen:
                        continue
                    seen.add(normalized)
                    metadata, error = await github.fetch_repo_metadata(normalized)
                    if error:
                        print(f"{normalized}: {error}")
                        continue
                    if metadata is not None:
                        database.upsert_github_repo(metadata)
                        print(f"{normalized}: enriched")

        total = len(papers)
        found_provider = 0
        found_final = 0
        ambiguous = 0
        rows: list[dict[str, object]] = []
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
            repo_metadata_by_url = {
                link.normalized_repo_url: metadata
                for link in final_links
                if (metadata := database.get_github_repo(link.normalized_repo_url)) is not None
            }
            rows.append(build_export_row(paper, final_links, repo_metadata_by_url))

        write_papers_csv(rows, OUTPUT_PATH)
        print(json.dumps({
            "papers": total,
            "provider_visible_link_papers": found_provider,
            "final_found_papers": found_final,
            "ambiguous_papers": ambiguous,
            "csv": str(OUTPUT_PATH),
            "paper_ids": len(paper_ids),
        }, ensure_ascii=False, indent=2))
        return 0
    finally:
        database.close()


async def _sync_paper(database, raw_cache, arxiv_links, huggingface, alphaxiv, github_search, paper):
    from src.ghstars.cli import (
        _has_found_repo,
        _sync_alphaxiv_link_surfaces,
        _sync_arxiv_link_surfaces,
        _sync_github_title_search_surface,
        _sync_huggingface_exact_surfaces,
        _sync_huggingface_title_search_surfaces,
    )

    await _sync_arxiv_link_surfaces(database, raw_cache, arxiv_links, paper.arxiv_id, paper.comment, paper.title)
    await _sync_huggingface_exact_surfaces(database, raw_cache, huggingface, paper.arxiv_id, paper.title)
    await _sync_alphaxiv_link_surfaces(database, raw_cache, alphaxiv, paper.arxiv_id, paper.title)

    observations = database.list_repo_observations(paper.arxiv_id)
    if not _has_found_repo(observations):
        await _sync_huggingface_title_search_surfaces(database, raw_cache, huggingface, paper.arxiv_id, paper.title)
        observations = database.list_repo_observations(paper.arxiv_id)
    if not _has_found_repo(observations):
        await _sync_github_title_search_surface(database, github_search, paper.arxiv_id, paper.title)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
