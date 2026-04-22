from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from papertorepo.db.models import Job, JobStatus, utc_now


STOP_REASON_USER_REQUESTED = "user_requested"
STOP_REQUESTED_MESSAGE = "Stop requested by user."
STOPPED_MESSAGE = "Stopped by user."


class JobStopRequested(RuntimeError):
    def __init__(self, job_id: str) -> None:
        super().__init__(STOPPED_MESSAGE)
        self.job_id = job_id


def request_job_stop(job: Job) -> bool:
    if job.stop_requested_at is not None:
        return False
    job.stop_requested_at = utc_now()
    job.stop_reason = STOP_REASON_USER_REQUESTED
    return True


def mark_job_cancelled(
    job: Job,
    *,
    clear_lock: bool = False,
    message: str = STOPPED_MESSAGE,
) -> None:
    request_job_stop(job)
    finished_at = utc_now()
    job.status = JobStatus.cancelled
    job.finished_at = finished_at
    job.error_text = message
    if clear_lock:
        job.locked_by = None
        job.locked_at = None
    else:
        job.locked_at = finished_at


def stop_requested_for_job(db: Session, job_id: str) -> bool:
    row = db.execute(select(Job.stop_requested_at, Job.status).where(Job.id == job_id)).one_or_none()
    if row is None:
        return True
    return row.status == JobStatus.cancelled or row.stop_requested_at is not None


def raise_if_job_stop_requested(db: Session, job_id: str) -> None:
    if stop_requested_for_job(db, job_id):
        raise JobStopRequested(job_id)
