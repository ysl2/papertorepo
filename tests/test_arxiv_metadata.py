import pytest

from src.ghstars.providers.arxiv_metadata import ArxivMetadataClient, parse_papers_from_feed


def test_parse_papers_from_feed_extracts_core_fields():
    feed = """<?xml version='1.0' encoding='UTF-8'?>
    <feed xmlns='http://www.w3.org/2005/Atom'>
      <entry>
        <id>http://arxiv.org/abs/2603.12345v1</id>
        <updated>2026-03-25T00:00:00Z</updated>
        <published>2026-03-24T00:00:00Z</published>
        <title> Test Paper </title>
        <summary> Example abstract. </summary>
        <author><name>Alice</name></author>
        <author><name>Bob</name></author>
        <category term='cs.CV' />
        <category term='cs.LG' />
        <arxiv:comment xmlns:arxiv='http://arxiv.org/schemas/atom'>Code: https://github.com/foo/bar</arxiv:comment>
      </entry>
    </feed>"""
    papers = parse_papers_from_feed(feed)
    assert len(papers) == 1
    paper = papers[0]
    assert paper.arxiv_id == "2603.12345"
    assert paper.abs_url == "https://arxiv.org/abs/2603.12345"
    assert paper.title == "Test Paper"
    assert paper.abstract == "Example abstract."
    assert paper.authors == ("Alice", "Bob")
    assert paper.categories == ("cs.CV", "cs.LG")
    assert paper.primary_category == "cs.CV"


@pytest.mark.anyio
async def test_fetch_id_list_feed_requests_all_ids(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_request_text(session, url, *, params, semaphore, rate_limiter, retry_prefix, max_retries=None):
        captured["session"] = session
        captured["url"] = url
        captured["params"] = dict(params)
        captured["retry_prefix"] = retry_prefix
        captured["max_retries"] = max_retries
        return 200, "<feed />", {"Content-Type": "application/atom+xml"}, None

    monkeypatch.setattr("src.ghstars.providers.arxiv_metadata.request_text", fake_request_text)

    client = ArxivMetadataClient(session=object(), min_interval=0.5)
    status, body, headers, error = await client.fetch_id_list_feed(["2503.00001", "2503.00002", "2503.00003"])

    assert (status, body, headers, error) == (200, "<feed />", {"Content-Type": "application/atom+xml"}, None)
    assert captured["url"] == "https://export.arxiv.org/api/query"
    assert captured["params"] == {
        "id_list": "2503.00001,2503.00002,2503.00003",
        "max_results": "3",
    }
    assert captured["retry_prefix"] == "arXiv metadata id_list query"
