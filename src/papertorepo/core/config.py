from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "papertorepo"
    api_prefix: str = "/api/v1"

    database_url: str = "postgresql+psycopg://papertorepo:papertorepo@db:5432/papertorepo"
    data_dir: Path = Path("data")
    raw_fetch_dir_name: str = "raw"
    export_dir_name: str = "exports"
    frontend_dist_dir: Path = Path("frontend/dist")
    sql_search_mode: Literal["off", "read_only", "read_write"] = "off"

    default_categories: str = "cs.CV"

    github_token: str = ""
    huggingface_token: str = ""
    alphaxiv_token: str = ""

    sync_papers_arxiv_min_interval: float = 3.0
    sync_papers_arxiv_ttl_days: int = 30
    sync_papers_arxiv_id_batch_size: int = 100
    sync_papers_arxiv_list_page_size: int = 2000

    find_repos_link_ttl_days: int = 7
    find_repos_huggingface_enabled: bool = True
    find_repos_alphaxiv_enabled: bool = True
    find_repos_huggingface_min_interval: float = 0.2
    find_repos_alphaxiv_min_interval: float = 0.2
    find_repos_worker_concurrency: int = 24
    find_repos_huggingface_max_concurrent: int = 4
    find_repos_alphaxiv_max_concurrent: int = 4

    refresh_metadata_github_min_interval: float = 0.2
    refresh_metadata_github_graphql_batch_size: int = 50
    refresh_metadata_github_rest_fallback_max_concurrent: int = 2

    job_queue_worker_poll_seconds: float = 1.0
    job_queue_running_timeout_seconds: int = 1800

    public_export_downloads: bool = True
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    @property
    def raw_fetch_dir(self) -> Path:
        return self.data_dir / self.raw_fetch_dir_name

    @property
    def export_dir(self) -> Path:
        return self.data_dir / self.export_dir_name

    @property
    def default_categories_list(self) -> list[str]:
        return [item.strip() for item in self.default_categories.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
