from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from papertorepo.api.app import app
from papertorepo.api.routes import (
    _cancel_sql_request,
    _execute_driver_sql,
    _register_sql_request,
    _sql_column_sources_from_pgresult,
    _unregister_sql_request,
)
from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import session_scope
from papertorepo.db.models import Job, JobStatus, JobType, Paper, utc_now


@pytest.fixture(autouse=True)
def enable_sql_mode(monkeypatch):
    monkeypatch.setenv("SQL_SEARCH_MODE", "read_write")
    clear_settings_cache()
    yield
    clear_settings_cache()


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


def test_sql_endpoint_is_disabled_when_configured_off(db_env, monkeypatch):
    monkeypatch.setenv("SQL_SEARCH_MODE", "off")
    clear_settings_cache()

    from papertorepo.api.app import create_app

    with TestClient(create_app()) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT 1"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["has_result_set"] is False
    assert body["message"] == "SQL search is disabled"


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


def test_sql_read_only_mode_allows_select(db_env, monkeypatch):
    monkeypatch.setenv("SQL_SEARCH_MODE", "read_only")
    clear_settings_cache()
    insert_test_paper("2604.00001")

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT title FROM papers WHERE arxiv_id = '2604.00001'"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["has_result_set"] is True
    assert body["rows"][0]["title"] == "Paper 2604.00001"


def test_sql_read_only_mode_rejects_update(db_env, monkeypatch):
    monkeypatch.setenv("SQL_SEARCH_MODE", "read_only")
    clear_settings_cache()
    insert_test_paper("2604.00001")

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/sql",
            json={"query": "UPDATE papers SET title = 'Updated' WHERE arxiv_id = '2604.00001'"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["has_result_set"] is False
    assert body["message"] is not None
    assert "readonly" in body["message"].lower() or "read-only" in body["message"].lower()

    with TestClient(app) as client:
        select_response = client.post("/api/v1/sql", json={"query": "SELECT title FROM papers WHERE arxiv_id = '2604.00001'"})
    assert select_response.json()["rows"][0]["title"] == "Paper 2604.00001"


def test_sql_read_only_mode_rejects_multiple_statements(db_env, monkeypatch):
    monkeypatch.setenv("SQL_SEARCH_MODE", "read_only")
    clear_settings_cache()

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT 1; SELECT 2"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["message"] is not None
    assert "one statement" in body["message"].lower() or "multiple" in body["message"].lower()


def test_sql_read_write_mode_rejects_when_job_is_running(db_env):
    with session_scope() as db:
        db.add(
            Job(
                job_type=JobType.sync_papers,
                status=JobStatus.running,
                attempt_series_key="running-sql-block",
                scope_json={},
                dedupe_key="running-sql-block",
            )
        )

    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT 1"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["has_result_set"] is False
    assert body["message"] == "Read-write SQL is disabled while jobs are running or stopping"


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


class FakeSqlCursor:
    def __init__(self) -> None:
        self.set_index = 0
        self.statusmessage = "SELECT 1"
        self.description = [("first_value",)]

    def fetchall(self):
        if self.set_index == 0:
            return [(1,)]
        return [(2,)]

    def nextset(self) -> bool:
        if self.set_index == 0:
            self.set_index = 1
            self.statusmessage = "SELECT 1"
            self.description = [("second_value",)]
            return True
        return False


def test_sql_response_from_cursor_returns_last_result_set():
    from papertorepo.api.routes import _sql_response_from_cursor

    response = _sql_response_from_cursor(FakeSqlCursor())

    assert response.ok is True
    assert response.has_result_set is True
    assert response.columns == ["second_value"]
    assert response.rows == [{"second_value": 2}]


def test_sql_request_id_cleans_up_after_execution(db_env):
    with TestClient(app) as client:
        response = client.post("/api/v1/sql", json={"query": "SELECT 1", "request_id": "req-cleanup"})

    assert response.status_code == 200
    assert response.json()["ok"] is True

    with TestClient(app) as client:
        cancel_response = client.post("/api/v1/sql/req-cleanup/cancel")

    assert cancel_response.status_code == 200
    cancel_body = cancel_response.json()
    assert cancel_body["ok"] is True
    assert cancel_body["request_id"] == "req-cleanup"
    assert cancel_body["cancel_requested"] is False


def test_sql_duplicate_active_request_id_is_rejected(db_env):
    request_id = "req-duplicate"
    assert _register_sql_request(request_id, None) is True
    try:
        with session_scope() as db:
            response = _execute_driver_sql(db, "SELECT 1", request_id=request_id)
    finally:
        _unregister_sql_request(request_id)

    assert response.ok is False
    assert response.message is not None
    assert "already running" in response.message


class FakeCancelableConnection:
    def __init__(self) -> None:
        self.cancel_safe_calls: list[float] = []

    def cancel_safe(self, *, timeout: float) -> None:
        self.cancel_safe_calls.append(timeout)


def test_sql_cancel_running_request_calls_cancel_safe():
    request_id = "req-cancel-safe"
    connection = FakeCancelableConnection()
    assert _register_sql_request(request_id, connection) is True
    try:
        response = _cancel_sql_request(request_id)
    finally:
        _unregister_sql_request(request_id)

    assert response.ok is True
    assert response.request_id == request_id
    assert response.cancel_requested is True
    assert connection.cancel_safe_calls == [2.0]


def test_sql_cancel_unknown_request_is_benign():
    with TestClient(app) as client:
        response = client.post("/api/v1/sql/unknown-sql-request/cancel")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["request_id"] == "unknown-sql-request"
    assert body["cancel_requested"] is False


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
