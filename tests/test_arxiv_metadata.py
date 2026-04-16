from src.ghstars.providers.arxiv_metadata import parse_papers_from_feed


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
