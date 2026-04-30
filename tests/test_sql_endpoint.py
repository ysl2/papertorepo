from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from papertorepo.api.app import app
from papertorepo.api.routes import _sql_column_sources_from_pgresult
from papertorepo.db.session import session_scope
from papertorepo.db.models import Paper, utc_now


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def insert_test_paper(arxiv_id: str = "2604.12345") -> None:
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


def null_source() -> dict[str, None]:
    return {
        "source_schema": None,
        "source_table": None,
        "source_column": None,
    }


def test_sql_select_returns_rows(db_env):
    insert_test_paper("2604.00001")
    insert_test_paper("2604.00002")

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT arxiv_id, title FROM papers ORDER BY arxiv_id"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["has_result_set"] is True
    assert body["columns"] == ["arxiv_id", "title"]
    assert body["column_sources"] == [null_source(), null_source()]
    assert body["row_count"] == 2
    assert body["rows"][0]["arxiv_id"] == "2604.00001"
    assert body["rows"][1]["arxiv_id"] == "2604.00002"


def test_sql_select_empty_result(db_env):
    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT arxiv_id FROM papers"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["has_result_set"] is True
    assert body["columns"] == ["arxiv_id"]
    assert body["column_sources"] == [null_source()]
    assert body["rows"] == []
    assert body["row_count"] == 0


def test_sql_duplicate_column_names_are_disambiguated(db_env):
    insert_test_paper("2604.00001")

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT arxiv_id, arxiv_id FROM papers"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["columns"] == ["arxiv_id", "arxiv_id_2"]
    assert body["column_sources"] == [null_source(), null_source()]
    assert body["rows"][0]["arxiv_id"] == "2604.00001"
    assert body["rows"][0]["arxiv_id_2"] == "2604.00001"


def test_sql_invalid_syntax_returns_error(db_env):
    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELEC bad syntax"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["has_result_set"] is False
    assert body["column_sources"] == []
    assert body["message"] is not None
    assert len(body["message"]) > 0


def test_sql_update_returns_no_result_set(db_env):
    insert_test_paper("2604.00001")

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/sql",
            json={"query": "UPDATE papers SET title = 'Updated' WHERE arxiv_id = '2604.00001'"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["has_result_set"] is False
    assert body["column_sources"] == []
    assert body["message"] is not None

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT title FROM papers WHERE arxiv_id = '2604.00001'"})
    assert response.json()["rows"][0]["title"] == "Updated"


def test_sql_nonexistent_table_returns_error(db_env):
    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT * FROM nonexistent_table_xyz"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["has_result_set"] is False
    assert body["column_sources"] == []
    assert body["message"] is not None


def test_sql_empty_query_returns_error(db_env):
    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": ""})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["column_sources"] == []


def test_sql_datetime_columns_serialized_as_strings(db_env):
    insert_test_paper("2604.00001")

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT arxiv_id, published_at FROM papers"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    row = body["rows"][0]
    assert isinstance(row["published_at"], str)


class FakePgResult:
    def __init__(self, refs: list[tuple[int, int]]) -> None:
        self.refs = refs

    def ftable(self, index: int) -> int:
        return self.refs[index][0]

    def ftablecol(self, index: int) -> int:
        return self.refs[index][1]


def test_sql_pgresult_column_sources_preserve_order_and_unknowns():
    pgresult = FakePgResult([(101, 1), (102, 1), (0, 0), (103, 1), (101, 1)])

    def lookup(keys: set[tuple[int, int]]) -> dict[tuple[int, int], dict[str, str | None]]:
        assert keys == {(101, 1), (102, 1), (103, 1)}
        return {
            (101, 1): {"source_schema": "public", "source_table": "papers", "source_column": "arxiv_id"},
            (102, 1): {"source_schema": "public", "source_table": "jobs", "source_column": "id"},
            (103, 1): {"source_schema": "public", "source_table": "exports", "source_column": "id"},
        }

    assert _sql_column_sources_from_pgresult(pgresult, 5, lookup) == [
        {"source_schema": "public", "source_table": "papers", "source_column": "arxiv_id"},
        {"source_schema": "public", "source_table": "jobs", "source_column": "id"},
        null_source(),
        {"source_schema": "public", "source_table": "exports", "source_column": "id"},
        {"source_schema": "public", "source_table": "papers", "source_column": "arxiv_id"},
    ]
