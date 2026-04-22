from papertorepo.core.normalize.arxiv import extract_arxiv_id_from_single_paper_url, normalize_arxiv_url
from papertorepo.core.normalize.github import extract_github_repo_urls, normalize_github_url


def test_normalize_arxiv_abs_url():
    assert normalize_arxiv_url("https://arxiv.org/abs/2603.12345v2") == "https://arxiv.org/abs/2603.12345"


def test_reject_non_arxiv_single_url():
    assert extract_arxiv_id_from_single_paper_url("https://example.com/abs/2603.12345") is None


def test_normalize_github_repo_url():
    assert normalize_github_url("https://github.com/foo/bar.git") == "https://github.com/foo/bar"


def test_extract_github_repo_urls_from_text():
    urls = extract_github_repo_urls("Code: https://github.com/foo/bar and https://github.com/foo/bar/issues")
    assert urls == ("https://github.com/foo/bar",)
