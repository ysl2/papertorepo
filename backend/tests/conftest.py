from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import configure_database, session_scope
from papertorepo.jobs.queue import init_database
from papertorepo.db.models import Paper, utc_now


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


@pytest.fixture
def db_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'papertorepo-test.db'}")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DEFAULT_CATEGORIES", "cs.CV")
    clear_settings_cache()
    configure_database()
    init_database()
    yield tmp_path
    clear_settings_cache()


def insert_paper(arxiv_id: str = "2604.12345") -> None:
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                title=f"Paper {arxiv_id}",
                abstract="Example abstract",
                published_at=at_utc_midnight(date(2026, 4, 18)),
                updated_at=at_utc_midnight(date(2026, 4, 18)),
                authors_json=["Alice", "Bob"],
                categories_json=["cs.CV"],
                comment=None,
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
