from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import String, func, literal
from sqlalchemy.sql import ColumnElement

from src.ghstarsv2.models import Job


def _scope_string(scope_json: dict[str, Any], key: str) -> str | None:
    value = scope_json.get(key)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def job_scope_window_sort_keys(scope_json: dict[str, Any]) -> tuple[str, str]:
    day = _scope_string(scope_json, "day")
    month = _scope_string(scope_json, "month")
    from_date = _scope_string(scope_json, "from")
    to_date = _scope_string(scope_json, "to")

    month_start = f"{month}-01" if month else None
    month_end = f"{month}-31" if month else None

    start_key = day or from_date or month_start or to_date or ""
    end_key = day or to_date or month_end or from_date or ""
    return start_key, end_key


def _job_scope_string_expr(key: str) -> ColumnElement[str | None]:
    return Job.scope_json[key].as_string()


def _job_scope_window_start_expr() -> ColumnElement[str]:
    month_expr = _job_scope_string_expr("month")
    empty = literal("", type_=String())
    return func.coalesce(
        _job_scope_string_expr("day"),
        _job_scope_string_expr("from"),
        month_expr + literal("-01", type_=String()),
        _job_scope_string_expr("to"),
        empty,
    )


def _job_scope_window_end_expr() -> ColumnElement[str]:
    month_expr = _job_scope_string_expr("month")
    empty = literal("", type_=String())
    return func.coalesce(
        _job_scope_string_expr("day"),
        _job_scope_string_expr("to"),
        month_expr + literal("-31", type_=String()),
        _job_scope_string_expr("from"),
        empty,
    )


def job_display_order_by() -> tuple[ColumnElement[Any], ...]:
    return (
        Job.created_at.desc(),
        _job_scope_window_start_expr().desc(),
        _job_scope_window_end_expr().desc(),
        Job.id.desc(),
    )


def job_execution_order_by() -> tuple[ColumnElement[Any], ...]:
    return (
        Job.created_at.asc(),
        _job_scope_window_start_expr().asc(),
        _job_scope_window_end_expr().asc(),
        Job.id.asc(),
    )


def _coerce_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def job_display_sort_key(job: Job) -> tuple[datetime, str, str, str]:
    start_key, end_key = job_scope_window_sort_keys(job.scope_json or {})
    return (_coerce_datetime(job.created_at), start_key, end_key, job.id)
