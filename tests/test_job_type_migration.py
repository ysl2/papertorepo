from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

from papertorepo.core.scope import build_dedupe_key


def _load_migration_module(file_name: str):
    migration_path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / file_name
    spec = importlib.util.spec_from_file_location(file_name.removesuffix(".py"), migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_alembic_revision_ids_fit_postgresql_version_column():
    version_dir = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    for migration_path in version_dir.glob("*.py"):
        module = _load_migration_module(migration_path.name)
        assert len(module.revision) <= 32, migration_path.name


def test_job_item_resume_progress_migration_creates_and_drops_table(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-resume-items.db'}")
    module = _load_migration_module("0013_job_item_resume_progress.py")

    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE jobs (id TEXT PRIMARY KEY)"))
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(module, "op", operations)

        module.upgrade()

        table_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        index_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'index'")).mappings()
        }
        assert "job_item_resume_progress" in table_names
        assert "ix_job_item_resume_progress_item" in index_names
        assert "ix_job_item_resume_progress_source_job" in index_names

        module.downgrade()

        table_names_after = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        assert "job_item_resume_progress" not in table_names_after


def test_github_url_metadata_migration_canonicalizes_and_deduplicates(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-github-url.db'}")
    module = _load_migration_module("0014_github_url_metadata.py")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE github_repos (
                    normalized_github_url TEXT PRIMARY KEY,
                    github_id INTEGER,
                    owner TEXT NOT NULL,
                    repo TEXT NOT NULL,
                    stars INTEGER,
                    created_at TEXT,
                    description TEXT,
                    homepage TEXT,
                    topics_json TEXT,
                    license TEXT,
                    archived BOOLEAN,
                    pushed_at TEXT,
                    first_seen_at DATETIME,
                    checked_at DATETIME,
                    etag TEXT,
                    last_modified TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO github_repos (
                    normalized_github_url, github_id, owner, repo, stars, created_at, description,
                    homepage, topics_json, license, archived, pushed_at, first_seen_at, checked_at,
                    etag, last_modified
                )
                VALUES
                    (
                        'https://github.com/Foo/Bar', 1, 'Foo', 'Bar', 10, '2020-01-01T00:00:00Z',
                        'old description', 'https://old.example', :old_topics, 'MIT', 0,
                        '2020-01-02T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                        'etag-old', 'modified-old'
                    ),
                    (
                        'https://github.com/foo/bar', 2, 'foo', 'bar', 20, '2020-02-01T00:00:00Z',
                        'new description', 'https://new.example', :new_topics, 'Apache-2.0', 1,
                        '2020-02-02T00:00:00Z', '2026-02-01T00:00:00Z', '2026-02-01T00:00:00Z',
                        'etag-new', 'modified-new'
                    )
                """
            ),
            {
                "old_topics": json.dumps(["old"]),
                "new_topics": json.dumps(["vision", "cv"]),
            },
        )
        connection.execute(
            text(
                """
                CREATE TABLE paper_repo_state (
                    arxiv_id TEXT PRIMARY KEY,
                    primary_repo_url TEXT,
                    repo_urls_json TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO paper_repo_state (arxiv_id, primary_repo_url, repo_urls_json)
                VALUES ('2604.12345', 'https://github.com/Foo/Bar.git', :repo_urls)
                """
            ),
            {"repo_urls": json.dumps(["https://github.com/Foo/Bar.git", "https://github.com/foo/bar/issues"])},
        )
        connection.execute(
            text(
                """
                CREATE TABLE repo_observations (
                    id INTEGER PRIMARY KEY,
                    observed_repo_url TEXT,
                    normalized_repo_url TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO repo_observations (id, observed_repo_url, normalized_repo_url)
                VALUES (1, 'https://github.com/Foo/Bar/issues/1', 'https://github.com/Foo/Bar.git')
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE job_item_resume_progress (
                    id TEXT PRIMARY KEY,
                    attempt_series_key TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    item_kind TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_job_id TEXT,
                    created_at DATETIME,
                    updated_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX ix_job_item_resume_progress_item
                ON job_item_resume_progress (attempt_series_key, job_type, item_kind, item_key)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO job_item_resume_progress (
                    id, attempt_series_key, job_type, item_kind, item_key, status, created_at, updated_at
                )
                VALUES (
                    'resume-1', 'series-1', 'refresh_metadata', 'repo',
                    'https://github.com/Foo/Bar.git', 'completed',
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
                )
                """
            )
        )

        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(module, "op", operations)
        module.upgrade()

        github_columns = {
            row.name
            for row in connection.execute(text("PRAGMA table_info(github_repos)")).mappings()
        }
        assert "github_url" in github_columns
        assert "stargazers_count" in github_columns
        assert "topic" in github_columns
        assert "normalized_github_url" not in github_columns
        assert "owner" not in github_columns
        assert "repo" not in github_columns
        assert "stars" not in github_columns
        assert "topics_json" not in github_columns
        assert "checked_at" not in github_columns
        assert "etag" not in github_columns
        assert "last_modified" not in github_columns

        github_rows = list(connection.execute(text("SELECT * FROM github_repos")).mappings())
        assert len(github_rows) == 1
        assert github_rows[0]["github_url"] == "https://github.com/foo/bar"
        assert github_rows[0]["github_id"] == 2
        assert github_rows[0]["name_with_owner"] == "foo/bar"
        assert github_rows[0]["stargazers_count"] == 20
        assert github_rows[0]["topic"] == "vision"
        assert github_rows[0]["is_archived"] == 1

        state = connection.execute(text("SELECT * FROM paper_repo_state")).mappings().one()
        assert state["primary_github_url"] == "https://github.com/foo/bar"
        assert json.loads(state["github_urls_json"]) == ["https://github.com/foo/bar"]

        observation = connection.execute(text("SELECT * FROM repo_observations")).mappings().one()
        assert observation["observed_github_url"] == "https://github.com/foo/bar"
        assert observation["github_url"] == "https://github.com/foo/bar"

        resume = connection.execute(text("SELECT item_key FROM job_item_resume_progress")).mappings().one()
        assert resume["item_key"] == "https://github.com/foo/bar"


def test_sync_papers_postgresql_enum_rename_does_not_reupdate_old_labels(monkeypatch):
    module = _load_migration_module("0011_rename_sync_papers.py")
    executed_sql: list[str] = []

    class FakeDialect:
        name = "postgresql"

    class FakeBind:
        dialect = FakeDialect()

        def exec_driver_sql(self, sql: str) -> None:
            executed_sql.append(sql)

    monkeypatch.setattr(module.op, "get_bind", lambda: FakeBind())
    monkeypatch.setattr(module, "_postgres_jobtype_labels", lambda: {"sync_arxiv", "sync_arxiv_batch"})

    mappings_requiring_update = module._prepare_postgresql_job_type_mapping(module.OLD_TO_NEW_JOB_TYPES)

    assert mappings_requiring_update == {}
    assert executed_sql == [
        "ALTER TYPE jobtype RENAME VALUE 'sync_arxiv' TO 'sync_papers'",
        "ALTER TYPE jobtype RENAME VALUE 'sync_arxiv_batch' TO 'sync_papers_batch'",
    ]


def test_upgrade_renames_repo_job_types_and_rebuilds_dedupe_keys(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    module = _load_migration_module("0010_rename_repo_job_types.py")

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


def test_upgrade_renames_sync_papers_job_types_tables_and_dedupe_keys(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-sync-papers.db'}")
    module = _load_migration_module("0011_rename_sync_papers.py")

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
                    ('job-sync', 'sync_arxiv', :sync_scope, 'old-sync'),
                    ('job-sync-batch', 'sync_arxiv_batch', :sync_batch_scope, 'old-sync-batch'),
                    ('job-find', 'find_repos', :find_scope, 'keep-me')
                """
            ),
            {
                "sync_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
                "sync_batch_scope": json.dumps({"categories": ["cs.CV"], "from": "2026-04-01", "to": "2026-05-31"}),
                "find_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-21"}),
            },
        )
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_sync_days (
                    category TEXT NOT NULL,
                    sync_day DATE NOT NULL,
                    last_completed_at DATETIME,
                    PRIMARY KEY (category, sync_day)
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX ix_arxiv_sync_days_completed ON arxiv_sync_days (last_completed_at)"))
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_archive_appearances (
                    arxiv_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    archive_month DATE NOT NULL,
                    observed_at DATETIME,
                    PRIMARY KEY (arxiv_id, category, archive_month)
                )
                """
            )
        )
        connection.execute(
            text("CREATE INDEX ix_arxiv_archive_appearances_month_arxiv ON arxiv_archive_appearances (archive_month, arxiv_id)")
        )
        connection.execute(
            text("CREATE INDEX ix_arxiv_archive_appearances_category_month ON arxiv_archive_appearances (category, archive_month)")
        )
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_sync_windows (
                    category TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    last_completed_at DATETIME,
                    PRIMARY KEY (category, start_date, end_date)
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX ix_arxiv_sync_windows_completed ON arxiv_sync_windows (last_completed_at)"))

        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(module, "op", operations)
        module.upgrade()

        rows = {
            row.id: row
            for row in connection.execute(text("SELECT id, job_type, scope_json, dedupe_key FROM jobs")).mappings()
        }
        table_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        index_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'index'")).mappings()
        }

    assert rows["job-sync"]["job_type"] == "sync_papers"
    assert rows["job-sync-batch"]["job_type"] == "sync_papers_batch"
    assert rows["job-find"]["job_type"] == "find_repos"
    assert rows["job-sync"]["dedupe_key"] == build_dedupe_key("sync_papers", json.loads(rows["job-sync"]["scope_json"]))
    assert rows["job-sync-batch"]["dedupe_key"] == build_dedupe_key(
        "sync_papers_batch",
        json.loads(rows["job-sync-batch"]["scope_json"]),
    )
    assert rows["job-find"]["dedupe_key"] == "keep-me"
    assert "sync_papers_arxiv_days" in table_names
    assert "sync_papers_arxiv_archive_appearances" in table_names
    assert "arxiv_sync_windows" not in table_names
    assert "ix_sync_papers_arxiv_days_completed" in index_names
    assert "ix_sync_papers_arxiv_archive_appearances_month_arxiv" in index_names
    assert "ix_sync_papers_arxiv_archive_appearances_category_month" in index_names
