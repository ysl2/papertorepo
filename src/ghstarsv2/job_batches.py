from __future__ import annotations

from typing import Any

from src.ghstarsv2.models import JobType
from src.ghstarsv2.scope import (
    arxiv_scope_spans_multiple_months,
    expand_arxiv_child_scope_jsons,
    expand_month_priority_child_scope_jsons,
)


BATCH_ROOT_JOB_TYPE_BY_CHILD: dict[JobType, JobType] = {
    JobType.sync_arxiv: JobType.sync_arxiv_batch,
    JobType.sync_links: JobType.sync_links_batch,
    JobType.enrich: JobType.enrich_batch,
}

BATCH_CHILD_JOB_TYPE_BY_ROOT: dict[JobType, JobType] = {
    root_job_type: child_job_type for child_job_type, root_job_type in BATCH_ROOT_JOB_TYPE_BY_CHILD.items()
}


def batch_root_job_type_for_child(job_type: JobType) -> JobType | None:
    return BATCH_ROOT_JOB_TYPE_BY_CHILD.get(job_type)


def batch_child_job_type_for_root(job_type: JobType) -> JobType | None:
    return BATCH_CHILD_JOB_TYPE_BY_ROOT.get(job_type)


def is_batch_root_job_type(job_type: JobType) -> bool:
    return job_type in BATCH_CHILD_JOB_TYPE_BY_ROOT


def is_batch_root_job(job_type: JobType, parent_job_id: str | None) -> bool:
    return parent_job_id is None and is_batch_root_job_type(job_type)


def planned_child_scope_jsons(job_type: JobType, scope_json: dict[str, Any]) -> list[dict[str, Any]]:
    child_job_type = batch_child_job_type_for_root(job_type) or job_type
    if child_job_type == JobType.sync_arxiv:
        return expand_arxiv_child_scope_jsons(scope_json)
    if child_job_type in {JobType.sync_links, JobType.enrich}:
        return expand_month_priority_child_scope_jsons(scope_json)
    return []


def should_create_batch_root(job_type: JobType, scope_json: dict[str, Any]) -> bool:
    if job_type == JobType.sync_arxiv:
        return arxiv_scope_spans_multiple_months(scope_json)
    if job_type in {JobType.sync_links, JobType.enrich}:
        return len(planned_child_scope_jsons(job_type, scope_json)) > 1
    return False
