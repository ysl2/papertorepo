from papertorepo.providers.huggingface_links import (
    extract_github_url_from_hf_payload,
)


def test_extract_github_url_from_payload():
    payload = '{"githubRepo": "https://github.com/foo/bar"}'
    assert extract_github_url_from_hf_payload(payload) == ("https://github.com/foo/bar",)
