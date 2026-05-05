from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import ValidationError
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from papertorepo.core.config import get_settings
from papertorepo.db.session import get_db
from papertorepo.jobs.ordering import job_display_order_by
from papertorepo.jobs.queue import (
    create_job,
    get_job_attempt_meta,
    launch_sync_job,
    list_job_attempts_read,
    list_jobs_read,
    rerun_job,
    stop_job,
    serialize_job,
    serialize_jobs,
)
from papertorepo.db.models import ExportRecord, GitHubRepo, Job, JobStatus, JobType, Paper, RepoStableStatus
from papertorepo.api.schemas import (
    DashboardStats,
    ExportRead,
    HealthRead,
    JobLaunchRead,
    JobQueueSummaryRead,
    JobRead,
    PaperRead,
    PaperSummaryRead,
    RepoRead,
    RuntimeConfigRead,
    ScopePayload,
    SqlColumnSource,
    SqlCancelResponse,
    SqlRequest,
    SqlResponse,
)
from papertorepo.core.scope import build_scope_json
from papertorepo.services.pipeline import (
    REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS,
    get_dashboard_stats,
    get_job_queue_snapshot,
    scoped_papers,
    scoped_repos,
)

logger = logging.getLogger(__name__)
SQL_CANCEL_TIMEOUT_SECONDS = 2.0
_sql_running_requests_lock = Lock()
_sql_running_requests: dict[str, object | None] = {}


def _step_providers() -> dict[str, list[str]]:
    return {
        "sync_papers": ["arxiv_listing", "arxiv_catchup", "arxiv_submitted_day", "arxiv_id_list"],
        "find_repos": ["paper_comment", "paper_abstract", "alphaxiv_api", "alphaxiv_html", "huggingface_api"],
        "refresh_metadata": ["github_api"],
    }


def _scope_from_query(
    *,
    categories: str | None,
    day: date | None,
    month: str | None,
    from_date: date | None,
    to_date: date | None,
    force: bool = False,
    output_name: str | None = None,
) -> ScopePayload:
    return ScopePayload(
        categories=categories or "",
        day=day,
        month=month,
        **{"from": from_date, "to": to_date},
        force=force,
        output_name=output_name,
    )


def _paper_summary_payload(paper: Paper, *, primary_github_repo: GitHubRepo | None = None) -> dict[str, object]:
    state = paper.repo_state
    return {
        "arxiv_id": paper.arxiv_id,
        "abs_url": paper.abs_url,
        "title": paper.title,
        "published_at": paper.published_at,
        "updated_at": paper.updated_at,
        "authors_json": paper.authors_json or [],
        "categories_json": paper.categories_json or [],
        "primary_category": paper.primary_category,
        "comment": paper.comment,
        "journal_ref": paper.journal_ref,
        "link_status": state.stable_status if state is not None else RepoStableStatus.unknown,
        "primary_github_url": state.primary_github_url if state is not None else None,
        "primary_github_stargazers_count": primary_github_repo.stargazers_count if primary_github_repo is not None else None,
        "primary_github_language": primary_github_repo.primary_language if primary_github_repo is not None else None,
        "primary_github_size_kb": primary_github_repo.size_kb if primary_github_repo is not None else None,
        "primary_github_created_at": primary_github_repo.created_at if primary_github_repo is not None else None,
        "primary_github_pushed_at": primary_github_repo.pushed_at if primary_github_repo is not None else None,
        "primary_github_updated_at": primary_github_repo.updated_at if primary_github_repo is not None else None,
        "primary_github_description": primary_github_repo.description if primary_github_repo is not None else None,
        "stable_decided_at": state.stable_decided_at if state is not None else None,
        "refresh_after": state.refresh_after if state is not None else None,
        "last_attempt_at": state.last_attempt_at if state is not None else None,
        "last_attempt_complete": bool(state.last_attempt_complete) if state is not None else False,
        "last_attempt_error": state.last_attempt_error if state is not None else None,
    }


def serialize_paper_summary(paper: Paper, *, primary_github_repo: GitHubRepo | None = None) -> PaperSummaryRead:
    return PaperSummaryRead(**_paper_summary_payload(paper, primary_github_repo=primary_github_repo))


def serialize_paper(paper: Paper, *, primary_github_repo: GitHubRepo | None = None) -> PaperRead:
    state = paper.repo_state
    return PaperRead(
        **_paper_summary_payload(paper, primary_github_repo=primary_github_repo),
        abstract=paper.abstract,
        doi=paper.doi,
        github_urls=state.github_urls_json if state is not None else [],
    )


def _enqueue_job(db: Session, job_type: JobType, scope: ScopePayload) -> JobRead:
    try:
        return serialize_job(db, create_job(db, job_type, scope))
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _launch_sync_job(db: Session, job_type: JobType, scope: ScopePayload) -> JobLaunchRead:
    try:
        job = launch_sync_job(db, job_type, scope)
        return JobLaunchRead(
            disposition="created",
            job=serialize_job(db, job),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _scope_error_detail(exc: ValueError | ValidationError) -> str:
    if isinstance(exc, ValidationError):
        errors = exc.errors()
        if errors:
            message = str(errors[0].get("msg") or "Invalid scope")
            return message.removeprefix("Value error, ")
        return "Invalid scope"
    return str(exc)


def _scope_http_exception(exc: ValueError | ValidationError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=_scope_error_detail(exc))


def _scope_json_from_query(
    *,
    categories: str | None,
    day: date | None,
    month: str | None,
    from_date: date | None,
    to_date: date | None,
    force: bool = False,
    output_name: str | None = None,
) -> dict[str, object]:
    try:
        return build_scope_json(
            _scope_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
                force=force,
                output_name=output_name,
            )
        )
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _serialize_sql_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _serialize_sql_row(row: dict[str, object]) -> dict[str, object]:
    return {key: _serialize_sql_value(value) for key, value in row.items()}


def _empty_sql_column_source() -> dict[str, str | None]:
    return {
        "source_schema": None,
        "source_table": None,
        "source_column": None,
    }


def _sql_column_names(description: object) -> list[str]:
    if description is None:
        return []
    columns: list[str] = []
    used: set[str] = set()
    counts: dict[str, int] = {}
    for column in description:
        name = getattr(column, "name", None)
        if name is None:
            name = column[0]
        base_name = str(name or "column")
        counts[base_name] = counts.get(base_name, 0) + 1
        column_name = base_name if counts[base_name] == 1 else f"{base_name}_{counts[base_name]}"
        while column_name in used:
            counts[base_name] += 1
            column_name = f"{base_name}_{counts[base_name]}"
        used.add(column_name)
        columns.append(column_name)
    return columns


def _sql_column_sources_from_pgresult(
    pgresult: object | None,
    column_count: int,
    lookup: Callable[[set[tuple[int, int]]], dict[tuple[int, int], dict[str, str | None]]],
) -> list[dict[str, str | None]]:
    return _sql_column_sources_from_refs(_sql_column_source_refs_from_pgresult(pgresult, column_count), lookup)


def _sql_column_source_refs_from_pgresult(
    pgresult: object | None,
    column_count: int,
) -> list[tuple[int, int] | None]:
    if pgresult is None:
        return [None for _ in range(column_count)]

    ftable = getattr(pgresult, "ftable", None)
    ftablecol = getattr(pgresult, "ftablecol", None)
    if ftable is None or ftablecol is None:
        return [None for _ in range(column_count)]

    refs: list[tuple[int, int] | None] = []
    for index in range(column_count):
        table_oid = int(ftable(index) or 0)
        column_number = int(ftablecol(index) or 0)
        if table_oid <= 0 or column_number <= 0:
            refs.append(None)
            continue
        key = (table_oid, column_number)
        refs.append(key)
    return refs


def _sql_column_sources_from_refs(
    refs: list[tuple[int, int] | None],
    lookup: Callable[[set[tuple[int, int]]], dict[tuple[int, int], dict[str, str | None]]],
) -> list[dict[str, str | None]]:
    source_keys = {ref for ref in refs if ref is not None}
    sources_by_key = lookup(source_keys)
    return [sources_by_key.get(ref, _empty_sql_column_source()) if ref else _empty_sql_column_source() for ref in refs]


def _lookup_pg_column_sources(
    raw_connection: object,
    source_keys: set[tuple[int, int]],
) -> dict[tuple[int, int], dict[str, str | None]]:
    if not source_keys:
        return {}

    values_sql = ", ".join(["(%s, %s)"] * len(source_keys))
    params: list[int] = []
    for table_oid, column_number in sorted(source_keys):
        params.extend([table_oid, column_number])

    cursor = raw_connection.cursor()
    try:
        cursor.execute(
            f"""
            WITH source(field_table_oid, field_attnum) AS (
              VALUES {values_sql}
            )
            SELECT
              source.field_table_oid,
              source.field_attnum,
              namespace.nspname,
              class.relname,
              attribute.attname
            FROM source
            JOIN pg_catalog.pg_class AS class
              ON class.oid = source.field_table_oid::oid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = class.relnamespace
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = class.oid
             AND attribute.attnum = source.field_attnum::int2
            """,
            params,
        )
        return {
            (int(row[0]), int(row[1])): {
                "source_schema": str(row[2]),
                "source_table": str(row[3]),
                "source_column": str(row[4]),
            }
            for row in cursor.fetchall()
        }
    finally:
        cursor.close()


def _sql_column_source_refs(cursor: object, column_count: int) -> list[tuple[int, int] | None]:
    pgresult = getattr(cursor, "pgresult", None)
    return _sql_column_source_refs_from_pgresult(pgresult, column_count)


def _sql_column_sources(
    refs: list[tuple[int, int] | None],
    raw_connection: object | None,
) -> list[dict[str, str | None]]:
    if raw_connection is None:
        return [_empty_sql_column_source() for _ in refs]
    try:
        return _sql_column_sources_from_refs(
            refs,
            lambda keys: _lookup_pg_column_sources(raw_connection, keys),
        )
    except Exception as exc:
        logger.warning("SQL column provenance lookup failed: %s", exc)
        return [_empty_sql_column_source() for _ in refs]


def _sql_status_message(cursor: object) -> str | None:
    status = getattr(cursor, "statusmessage", None)
    if status:
        return str(status)
    rowcount = getattr(cursor, "rowcount", -1)
    if isinstance(rowcount, int) and rowcount >= 0:
        return f"{rowcount} row(s) affected"
    return None


def _sql_response_from_cursor(cursor: object, raw_connection: object | None = None) -> SqlResponse:
    has_result_set = False
    columns: list[str] = []
    column_source_refs: list[tuple[int, int] | None] = []
    rows: list[dict[str, object]] = []
    message = _sql_status_message(cursor)

    while True:
        description = getattr(cursor, "description", None)
        if description is not None:
            has_result_set = True
            columns = _sql_column_names(description)
            rows = [_serialize_sql_row(dict(zip(columns, row))) for row in cursor.fetchall()]
            column_source_refs = _sql_column_source_refs(cursor, len(columns))
            message = None
        else:
            has_result_set = False
            columns = []
            column_source_refs = []
            rows = []
            message = _sql_status_message(cursor)

        nextset = getattr(cursor, "nextset", None)
        if nextset is None or not nextset():
            break

    column_sources = _sql_column_sources(column_source_refs, raw_connection) if has_result_set else []
    return SqlResponse(
        ok=True,
        has_result_set=has_result_set,
        columns=columns,
        column_sources=[SqlColumnSource(**source) for source in column_sources],
        rows=rows,
        row_count=len(rows) if has_result_set else 0,
        message=message,
    )


def _sql_error_response(message: str) -> SqlResponse:
    return SqlResponse(
        ok=False,
        has_result_set=False,
        columns=[],
        column_sources=[],
        rows=[],
        row_count=0,
        message=message,
    )


def _driver_connection_from_session(db: Session) -> object:
    proxied_connection = db.connection().connection
    return getattr(
        proxied_connection,
        "driver_connection",
        getattr(proxied_connection, "dbapi_connection", proxied_connection),
    )


def _register_sql_request(request_id: str, raw_connection: object | None) -> bool:
    with _sql_running_requests_lock:
        if request_id in _sql_running_requests:
            return False
        _sql_running_requests[request_id] = raw_connection
        return True


def _unregister_sql_request(request_id: str) -> None:
    with _sql_running_requests_lock:
        _sql_running_requests.pop(request_id, None)


def _cancel_sql_request(request_id: str) -> SqlCancelResponse:
    with _sql_running_requests_lock:
        raw_connection = _sql_running_requests.get(request_id)

    if raw_connection is None:
        return SqlCancelResponse(
            ok=True,
            request_id=request_id,
            cancel_requested=False,
            message="SQL request is not running or is not cancelable",
        )

    try:
        cancel_safe = getattr(raw_connection, "cancel_safe", None)
        if callable(cancel_safe):
            cancel_safe(timeout=SQL_CANCEL_TIMEOUT_SECONDS)
        else:
            cancel = getattr(raw_connection, "cancel")
            cancel()
    except Exception as exc:
        logger.warning("SQL cancellation failed for request %s: %s", request_id, exc)
        return SqlCancelResponse(
            ok=False,
            request_id=request_id,
            cancel_requested=False,
            message=str(exc),
        )

    logger.info("SQL cancellation requested for %s", request_id)
    return SqlCancelResponse(
        ok=True,
        request_id=request_id,
        cancel_requested=True,
        message="Cancel requested",
    )


def _disable_read_only_sql_transaction(cursor: object, dialect_name: str | None) -> None:
    if dialect_name == "sqlite":
        cursor.execute("PRAGMA query_only = OFF")


def _has_running_or_stopping_job(db: Session) -> bool:
    return (
        db.query(Job.id)
        .filter(
            Job.finished_at.is_(None),
            or_(
                Job.status == JobStatus.running,
                Job.stop_requested_at.isnot(None),
            )
        )
        .limit(1)
        .first()
        is not None
    )


def _execute_read_only_sql(db: Session, query: str, request_id: str | None = None) -> SqlResponse:
    connection = db.connection()
    raw_connection = _driver_connection_from_session(db)
    dialect_name = db.bind.dialect.name if db.bind is not None else None
    is_postgresql = dialect_name == "postgresql"
    registered_request_id: str | None = None
    cursor = None

    if request_id:
        cancel_connection = raw_connection if is_postgresql else None
        if not _register_sql_request(request_id, cancel_connection):
            return _sql_error_response(f"SQL request {request_id} is already running")
        registered_request_id = request_id

    try:
        if dialect_name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        elif dialect_name == "sqlite":
            cursor = raw_connection.cursor()
            cursor.execute("PRAGMA query_only = ON")
        result = connection.exec_driver_sql(query)
        if result.returns_rows:
            columns = (
                _sql_column_names(result.cursor.description)
                if result.cursor is not None and result.cursor.description is not None
                else [str(key) for key in result.keys()]
            )
            rows = [_serialize_sql_row(dict(zip(columns, row))) for row in result.fetchall()]
            column_refs = _sql_column_source_refs(result.cursor, len(columns)) if result.cursor is not None else []
            column_sources = _sql_column_sources(column_refs, raw_connection)
            response = SqlResponse(
                ok=True,
                has_result_set=True,
                columns=columns,
                column_sources=[SqlColumnSource(**source) for source in column_sources],
                rows=rows,
                row_count=len(rows),
                message=None,
            )
        else:
            message = _sql_status_message(result.cursor) if result.cursor is not None else None
            response = SqlResponse(
                ok=True,
                has_result_set=False,
                columns=[],
                column_sources=[],
                rows=[],
                row_count=0,
                message=message,
            )
        db.rollback()
        if response.has_result_set:
            logger.info("Read-only SQL query returned %d rows, %d columns", response.row_count, len(response.columns))
        else:
            logger.info("Read-only SQL statement executed: %s", response.message or "no result set")
        return response
    except Exception as exc:
        db.rollback()
        error_message = str(exc)
        logger.warning("Read-only SQL execution failed: %s", error_message)
        return _sql_error_response(error_message)
    finally:
        if cursor is not None:
            try:
                _disable_read_only_sql_transaction(cursor, dialect_name)
            except Exception as exc:
                logger.warning("SQL read-only cleanup failed: %s", exc)
            cursor.close()
        if registered_request_id is not None:
            _unregister_sql_request(registered_request_id)


def _execute_driver_sql(
    db: Session,
    query: str,
    request_id: str | None = None,
    mode: Literal["off", "read_only", "read_write"] = "read_write",
) -> SqlResponse:
    if mode == "off":
        logger.info("SQL execution rejected because SQL search is disabled")
        return _sql_error_response("SQL search is disabled")
    if mode == "read_only":
        return _execute_read_only_sql(db, query, request_id=request_id)
    if mode == "read_write" and _has_running_or_stopping_job(db):
        logger.warning("Read-write SQL rejected because a job is running or stopping")
        return _sql_error_response("Read-write SQL is disabled while jobs are running or stopping")

    raw_connection = _driver_connection_from_session(db)
    dialect_name = db.bind.dialect.name if db.bind is not None else None
    is_postgresql = dialect_name == "postgresql"
    registered_request_id: str | None = None
    cursor = None

    if request_id:
        cancel_connection = raw_connection if is_postgresql else None
        if not _register_sql_request(request_id, cancel_connection):
            return _sql_error_response(f"SQL request {request_id} is already running")
        registered_request_id = request_id

    try:
        cursor = raw_connection.cursor()
        if dialect_name == "postgresql":
            cursor.execute(query, prepare=False)
        else:
            cursor.execute(query)
        response = _sql_response_from_cursor(cursor, raw_connection)
        db.commit()
        if response.has_result_set:
            logger.info("SQL query returned %d rows, %d columns", response.row_count, len(response.columns))
        else:
            logger.info("SQL statement executed: %s", response.message or "no result set")
        return response
    except Exception as exc:
        db.rollback()
        error_message = str(exc)
        logger.warning("SQL execution failed: %s", error_message)
        return _sql_error_response(error_message)
    finally:
        if cursor is not None:
            cursor.close()
        if registered_request_id is not None:
            _unregister_sql_request(registered_request_id)


def register_routes(app: FastAPI) -> None:
    settings = get_settings()
    router = APIRouter(prefix=settings.api_prefix)

    @router.get("/health", response_model=HealthRead)
    def health(db: Session = Depends(get_db)) -> HealthRead:
        dialect_name = db.bind.dialect.name
        settings = get_settings()
        github_auth_configured = bool(settings.github_token.strip())
        effective_github_min_interval_seconds = (
            settings.refresh_metadata_github_min_interval
            if github_auth_configured
            else max(
                settings.refresh_metadata_github_min_interval,
                REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS,
            )
        )
        return HealthRead(
            app_name=settings.app_name,
            api_prefix=settings.api_prefix,
            database_dialect=dialect_name,
            queue_mode="serial",
            github_auth_configured=github_auth_configured,
            effective_github_min_interval_seconds=effective_github_min_interval_seconds,
        )

    @router.get("/runtime-config", response_model=RuntimeConfigRead)
    def runtime_config() -> RuntimeConfigRead:
        settings = get_settings()
        return RuntimeConfigRead(
            default_categories=settings.default_categories_list,
            sql_search_mode=settings.sql_search_mode,
            step_providers=_step_providers(),
            active_dashboard_poll_ms=settings.frontend_active_dashboard_poll_ms,
            idle_dashboard_poll_ms=settings.frontend_idle_dashboard_poll_ms,
            active_jobs_poll_ms=settings.frontend_active_jobs_poll_ms,
            passive_jobs_poll_ms=settings.frontend_passive_jobs_poll_ms,
            table_refresh_poll_ms=settings.frontend_table_refresh_poll_ms,
            paper_batch_size=settings.frontend_paper_batch_size,
            repo_preview_limit=settings.frontend_repo_preview_limit,
            job_preview_limit=settings.frontend_job_preview_limit,
            displayed_keys_sync_throttle_ms=settings.frontend_displayed_keys_sync_throttle_ms,
            tooltip_show_delay_ms=settings.frontend_tooltip_show_delay_ms,
        )

    @router.get("/dashboard", response_model=DashboardStats)
    def public_dashboard(
        categories: str | None = Query(default=None),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        db: Session = Depends(get_db),
    ) -> DashboardStats:
        scope = _scope_json_from_query(
            categories=categories,
            day=day,
            month=month,
            from_date=from_date,
            to_date=to_date,
        )
        stats = get_dashboard_stats(db, scope)
        queue_snapshot = get_job_queue_snapshot(db)
        queue_job_ids = [job_id for job_id in [queue_snapshot.get("current_job_id"), queue_snapshot.get("next_job_id")] if job_id]
        queue_jobs_by_id: dict[str, JobRead] = {}
        if queue_job_ids:
            queue_jobs = list(db.scalars(select(Job).where(Job.id.in_(queue_job_ids))).all())
            queue_jobs_by_id = {job.id: serialize_job(db, job) for job in queue_jobs}
        recent_jobs = list(db.scalars(select(Job).order_by(*job_display_order_by()).limit(12)))
        return DashboardStats(
            **stats,
            job_queue_summary=JobQueueSummaryRead(
                state=str(queue_snapshot["state"]),
                running=stats["running_jobs"],
                pending=stats["pending_jobs"],
                stopping=stats["stopping_jobs"],
                current_job=queue_jobs_by_id.get(str(queue_snapshot["current_job_id"])) if queue_snapshot["current_job_id"] else None,
                next_job=queue_jobs_by_id.get(str(queue_snapshot["next_job_id"])) if queue_snapshot["next_job_id"] else None,
            ),
            recent_jobs=serialize_jobs(db, recent_jobs),
        )

    @router.get("/papers", response_model=list[PaperSummaryRead])
    def public_papers(
        categories: str | None = Query(default=None),
        status_filter: RepoStableStatus | None = Query(default=None, alias="status"),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=500, ge=1, le=25000),
        db: Session = Depends(get_db),
    ) -> list[PaperSummaryRead]:
        papers = scoped_papers(
            db,
            _scope_json_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
            ),
            offset=offset,
            limit=limit,
        )
        primary_github_urls = {
            paper.repo_state.primary_github_url
            for paper in papers
            if paper.repo_state is not None and paper.repo_state.primary_github_url is not None
        }
        repos_by_url = {
            repo.github_url: repo
            for repo in db.scalars(select(GitHubRepo).where(GitHubRepo.github_url.in_(primary_github_urls))).all()
        }
        rows = [
            serialize_paper_summary(
                paper,
                primary_github_repo=repos_by_url.get(paper.repo_state.primary_github_url)
                if paper.repo_state is not None and paper.repo_state.primary_github_url is not None
                else None,
            )
            for paper in papers
        ]
        if status_filter is not None:
            rows = [paper for paper in rows if paper.link_status == status_filter]
        return rows

    @router.get("/papers/{arxiv_id}", response_model=PaperRead)
    def public_paper(arxiv_id: str, db: Session = Depends(get_db)) -> PaperRead:
        paper = db.scalar(select(Paper).options(selectinload(Paper.repo_state)).where(Paper.arxiv_id == arxiv_id))
        if paper is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Paper not found")
        primary_github_url = paper.repo_state.primary_github_url if paper.repo_state is not None else None
        repo = db.get(GitHubRepo, primary_github_url) if primary_github_url is not None else None
        return serialize_paper(
            paper,
            primary_github_repo=repo,
        )

    @router.get("/repos", response_model=list[RepoRead])
    def public_repos(
        categories: str | None = Query(default=None),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        limit: int = Query(default=200, ge=1, le=10000),
        db: Session = Depends(get_db),
    ) -> list[RepoRead]:
        repos = scoped_repos(
            db,
            _scope_json_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
            ),
            limit=limit,
        )
        return [RepoRead.model_validate(item) for item in repos]

    @router.get("/exports", response_model=list[ExportRead])
    def public_exports(db: Session = Depends(get_db)) -> list[ExportRead]:
        return [ExportRead.model_validate(item) for item in db.scalars(select(ExportRecord).order_by(ExportRecord.created_at.desc())).all()]

    @router.get("/exports/{export_id}", response_model=ExportRead)
    def public_export(export_id: str, db: Session = Depends(get_db)) -> ExportRead:
        export_record = db.get(ExportRecord, export_id)
        if export_record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export not found")
        return ExportRead.model_validate(export_record)

    @router.get("/exports/{export_id}/download")
    def download_export(export_id: str, db: Session = Depends(get_db)) -> FileResponse:
        export_record = db.get(ExportRecord, export_id)
        if export_record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export not found")
        if not get_settings().public_export_downloads:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Public downloads disabled")
        path = Path(export_record.file_path)
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export file missing")
        return FileResponse(path, filename=export_record.file_name, media_type="text/csv")

    @router.get("/jobs", response_model=list[JobRead])
    def list_jobs(
        limit: int = Query(default=100, ge=1, le=1000),
        parent_id: str | None = Query(default=None),
        root_only: bool = Query(default=False),
        view: Literal["all", "latest"] = Query(default="all"),
        db: Session = Depends(get_db),
    ) -> list[JobRead]:
        return list_jobs_read(db, limit=limit, parent_job_id=parent_id, root_only=root_only, view=view)

    @router.get("/jobs/{job_id}", response_model=JobRead)
    def get_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        job = db.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        return serialize_job(db, job, attempt_meta=get_job_attempt_meta(db, job))

    @router.get("/jobs/{job_id}/attempts", response_model=list[JobRead])
    def get_job_attempts(
        job_id: str,
        limit: int = Query(default=100, ge=1, le=500),
        db: Session = Depends(get_db),
    ) -> list[JobRead]:
        try:
            return list_job_attempts_read(db, job_id, limit=limit)
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @router.post("/jobs/sync-papers", response_model=JobLaunchRead)
    def enqueue_sync_papers(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.sync_papers, scope)

    @router.post("/jobs/find-repos", response_model=JobLaunchRead)
    def enqueue_find_repos(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.find_repos, scope)

    @router.post("/jobs/refresh-metadata", response_model=JobLaunchRead)
    def enqueue_refresh_metadata(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.refresh_metadata, scope)

    @router.post("/jobs/export", response_model=JobRead)
    def enqueue_export(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_job(db, JobType.export, scope)

    @router.post("/jobs/{job_id}/rerun", response_model=JobRead)
    def rerun_existing_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        try:
            job = rerun_job(db, job_id)
            return serialize_job(db, job, attempt_meta=get_job_attempt_meta(db, job))
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    @router.post("/jobs/{job_id}/stop", response_model=JobRead)
    def stop_existing_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        try:
            return serialize_job(db, stop_job(db, job_id))
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @router.post("/sql", response_model=SqlResponse)
    def execute_sql(request: SqlRequest, db: Session = Depends(get_db)) -> SqlResponse:
        query = request.query.strip()
        if not query:
            return SqlResponse(
                ok=False,
                has_result_set=False,
                columns=[],
                column_sources=[],
                rows=[],
                row_count=0,
                message="Empty query",
            )
        return _execute_driver_sql(db, query, request_id=request.request_id, mode=get_settings().sql_search_mode)

    @router.post("/sql/{request_id}/cancel", response_model=SqlCancelResponse)
    def cancel_sql(request_id: str) -> SqlCancelResponse:
        return _cancel_sql_request(request_id)

    app.include_router(router)
