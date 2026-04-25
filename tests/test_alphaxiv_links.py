from papertorepo.providers.alphaxiv_links import (
    extract_github_url_from_alphaxiv_html,
    extract_github_url_from_alphaxiv_payload,
)


def test_extract_github_url_from_alphaxiv_payload_prefers_known_fields():
    payload = """
    {
      "paper": {
        "implementation": "https://github.com/foo/bar",
        "marimo_implementation": null,
        "paper_group": {"resources": []},
        "resources": []
      }
    }
    """
    assert extract_github_url_from_alphaxiv_payload(payload) == ("https://github.com/foo/bar",)


def test_extract_github_url_from_alphaxiv_html_reads_embedded_resource_repo():
    html = '''
    <a href="https://github.com/alphaxiv/feedback">Feedback</a>
    <script>
      resources:$R[1123]={github:$R[1124]={url:"https://github.com/YOUNG-bit/open_semantic_slam",description:"ICRA2025 repo"}}
    </script>
    '''
    assert extract_github_url_from_alphaxiv_html(html) == ("https://github.com/young-bit/open_semantic_slam",)
