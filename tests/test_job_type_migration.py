from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from sqlalchemy import create_engine, text

from papertorepo.core.scope import build_dedupe_key


def _load_migration_module():
    migration_path = (
        Path(__file__).resolve().parents[1] / "alembic" / "versions" / "0010_rename_repo_job_types.py"
    )
    spec = importlib.util.spec_from_file_location("rename_repo_job_types", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_upgrade_renames_repo_job_types_and_rebuilds_dedupe_keys(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    module = _load_migration_module()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    scope_json TEXT,
                    dedupe_key TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO jobs (id, job_type, scope_json, dedupe_key)
                VALUES
                    ('job-find', 'sync_links', :find_scope, 'old-find'),
                    ('job-find-batch', 'sync_links_batch', :find_batch_scope, 'old-find-batch'),
                    ('job-refresh', 'enrich', :refresh_scope, 'old-refresh'),
                    ('job-refresh-batch', 'enrich_batch', :refresh_batch_scope, 'old-refresh-batch'),
                    ('job-arxiv', 'sync_arxiv', :arxiv_scope, 'keep-me')
                """
            ),
            {
                "find_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-21"}),
                "find_batch_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
                "refresh_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-22"}),
                "refresh_batch_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-05"}),
                "arxiv_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
            },
        )

        monkeypatch.setattr(module.op, "get_bind", lambda: connection)
        module.upgrade()

        rows = {
            row.id: row
            for row in connection.execute(text("SELECT id, job_type, scope_json, dedupe_key FROM jobs")).mappings()
        }

    assert rows["job-find"]["job_type"] == "find_repos"
    assert rows["job-find-batch"]["job_type"] == "find_repos_batch"
    assert rows["job-refresh"]["job_type"] == "refresh_metadata"
    assert rows["job-refresh-batch"]["job_type"] == "refresh_metadata_batch"
    assert rows["job-arxiv"]["job_type"] == "sync_arxiv"

    assert rows["job-find"]["dedupe_key"] == build_dedupe_key("find_repos", json.loads(rows["job-find"]["scope_json"]))
    assert rows["job-find-batch"]["dedupe_key"] == build_dedupe_key(
        "find_repos_batch",
        json.loads(rows["job-find-batch"]["scope_json"]),
    )
    assert rows["job-refresh"]["dedupe_key"] == build_dedupe_key(
        "refresh_metadata",
        json.loads(rows["job-refresh"]["scope_json"]),
    )
    assert rows["job-refresh-batch"]["dedupe_key"] == build_dedupe_key(
        "refresh_metadata_batch",
        json.loads(rows["job-refresh-batch"]["scope_json"]),
    )
    assert rows["job-arxiv"]["dedupe_key"] == "keep-me"
