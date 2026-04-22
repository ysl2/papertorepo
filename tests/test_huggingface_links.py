from papertorepo.providers.huggingface_links import (
    extract_github_url_from_hf_html,
    extract_github_url_from_hf_payload,
    extract_paper_id_from_search_html,
    extract_paper_id_from_search_payload,
)


def test_extract_github_url_from_payload():
    payload = '{"githubRepo": "https://github.com/foo/bar"}'
    assert extract_github_url_from_hf_payload(payload) == ("https://github.com/foo/bar",)


def test_extract_github_url_from_html_json_field():
    html = '<script>var x = {"githubRepo":"https://github.com/foo/bar"};</script>'
    assert extract_github_url_from_hf_html(html) == ("https://github.com/foo/bar",)


def test_extract_github_url_from_html_anchor():
    html = '<a href="https://github.com/foo/bar" title="GitHub">GitHub</a>'
    assert extract_github_url_from_hf_html(html) == ("https://github.com/foo/bar",)


def test_extract_paper_id_from_search_html_matches_exact_title():
    html = """
    <div
      data-target="DailyPapers"
      data-props="{
        &quot;searchResults&quot;:[
          {
            &quot;title&quot;:&quot;Other Paper&quot;,
            &quot;paper&quot;:{&quot;id&quot;:&quot;2603.08055&quot;,&quot;title&quot;:&quot;Other Paper&quot;}
          },
          {
            &quot;title&quot;:&quot;Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass&quot;,
            &quot;paper&quot;:{&quot;id&quot;:&quot;2501.13928&quot;,&quot;title&quot;:&quot;Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass&quot;}
          }
        ]
      }">
    </div>
    """
    assert extract_paper_id_from_search_html(
        html,
        "Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass",
    ) == ("2501.13928", "title_search_huggingface_exact")


def test_extract_paper_id_from_search_payload_matches_exact_title():
    payload = '{"searchResults":[{"title":"Other Paper","paper":{"id":"2603.08055","title":"Other Paper"}},{"title":"Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass","paper":{"id":"2501.13928","title":"Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass"}}]}'
    assert extract_paper_id_from_search_payload(
        payload,
        "Fast3R: Towards 3D Reconstruction of 1000+ Images in One Forward Pass",
    ) == ("2501.13928", "title_search_huggingface_exact")
