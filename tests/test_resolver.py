from src.ghstars.associate.resolver import build_final_links, parity_summary
from src.ghstars.models import PaperRepoLink, RepoObservation


def make_observation(*, provider: str, surface: str, url: str | None, status: str = "found") -> RepoObservation:
    return RepoObservation(
        id=1,
        arxiv_id="2603.12345",
        provider=provider,
        surface=surface,
        status=status,
        observed_repo_url=url,
        normalized_repo_url=url,
        evidence_text=None,
        raw_cache_id=None,
        extractor_version="1",
        error_message=None,
        observed_at="2026-04-15T00:00:00+00:00",
    )


def test_build_final_links_prefers_multi_provider_repo():
    links = build_final_links(
        "2603.12345",
        [
            make_observation(provider="arxiv", surface="comment", url="https://github.com/foo/bar"),
            make_observation(provider="huggingface", surface="paper_api", url="https://github.com/foo/bar"),
            make_observation(provider="huggingface", surface="paper_html", url="https://github.com/other/repo"),
        ],
    )
    assert links[0]["normalized_repo_url"] == "https://github.com/foo/bar"
    assert links[0]["is_primary"] is True
    assert links[0]["status"] == "ambiguous"


def test_parity_summary_reports_found_status():
    observations = [make_observation(provider="arxiv", surface="comment", url="https://github.com/foo/bar")]
    final_links = [
        PaperRepoLink(
            id=1,
            arxiv_id="2603.12345",
            normalized_repo_url="https://github.com/foo/bar",
            status="found",
            providers=("arxiv",),
            surfaces=("arxiv:comment",),
            provider_count=1,
            surface_count=1,
            is_primary=True,
            resolved_at="2026-04-15T00:00:00+00:00",
        )
    ]
    summary = parity_summary(observations, final_links)
    assert summary["found_any_provider_link"] is True
    assert summary["final_status"] == "found"
