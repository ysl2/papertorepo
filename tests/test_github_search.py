from datetime import date

import pytest

from src.ghstars.providers.github_search import (
    GitHubRepositorySearchClient,
    RepositorySearchRow,
    SearchPartition,
    SearchRequest,
    collect_repositories,
    resolve_github_search_min_interval,
)


def test_resolve_github_search_min_interval_respects_search_quotas():
    assert resolve_github_search_min_interval("", 0.2) == 6.0
    assert resolve_github_search_min_interval("gh_token", 0.2) == 2.0
    assert resolve_github_search_min_interval("", 10.0) == 10.0
    assert resolve_github_search_min_interval("gh_token", 3.0) == 3.0


class FakeResponse:
    def __init__(self, payload, *, status=200, headers=None, text=""):
        self.payload = payload
        self.status = status
        self.headers = headers or {}
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text if self._text else __import__("json").dumps(self.payload)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params=None, headers=None):
        self.calls.append(
            {
                "url": url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
            }
        )
        return self.responses.pop(0)


@pytest.mark.anyio
async def test_count_results_fails_when_github_search_reports_incomplete_results():
    client = GitHubRepositorySearchClient(
        FakeSession(
            [
                FakeResponse(
                    {"total_count": 1200, "incomplete_results": True, "items": []},
                )
            ]
        ),
        github_token="gh_token",
        max_concurrent=1,
        min_interval=0,
    )

    with pytest.raises(RuntimeError, match="incomplete_results"):
        await client.count_results(SearchPartition(request=SearchRequest(query="cvpr 2026")))


@pytest.mark.anyio
async def test_collect_repositories_splits_created_range_before_deeper_star_splits():
    start = date(2020, 1, 1)
    end = date(2020, 1, 10)
    request = SearchRequest(query="cvpr 2026")

    class FakeSearchClient:
        def __init__(self):
            self.count_calls = []

        async def count_results(self, partition):
            key = (
                partition.stars_min,
                partition.stars_max,
                partition.created_after,
                partition.created_before,
            )
            self.count_calls.append(key)
            if key == (0, 100, start, end):
                return 2001
            if key == (0, 50, start, end):
                return 2001
            return 10

        async def fetch_partition(self, partition):
            suffix = (
                f"{partition.stars_min}-{partition.stars_max}-"
                f"{partition.created_after.isoformat()}-{partition.created_before.isoformat()}"
            )
            return [
                RepositorySearchRow(
                    github=f"https://github.com/example/{suffix}",
                    stars=1,
                    about="",
                    created="2024-01-01T00:00:00Z",
                )
            ]

    client = FakeSearchClient()
    rows = await collect_repositories(
        client,
        request,
        default_created_after=start,
        default_created_before=end,
        default_stars_min=0,
        default_stars_max=100,
    )

    assert rows
    assert client.count_calls[:3] == [
        (0, 100, start, end),
        (0, 50, start, end),
        (0, 50, start, date(2020, 1, 5)),
    ]


@pytest.mark.anyio
async def test_search_by_paper_title_normalizes_and_deduplicates_results():
    client = GitHubRepositorySearchClient(FakeSession([]), github_token="gh_token", max_concurrent=1, min_interval=0)

    async def fake_collect(request, **_kwargs):
        assert request.query == '"Fast3R"'
        return [
            RepositorySearchRow(
                github="https://github.com/foo/bar.git",
                stars=5,
                about="repo",
                created="2024-01-01T00:00:00Z",
            ),
            RepositorySearchRow(
                github="https://github.com/foo/bar/issues",
                stars=4,
                about="duplicate",
                created="2024-01-02T00:00:00Z",
            ),
        ]

    client.collect_repositories = fake_collect
    candidates = await client.search_by_paper_title("Fast3R")

    assert len(candidates) == 1
    assert candidates[0].normalized_repo_url == "https://github.com/foo/bar"
    assert candidates[0].stars == 5
