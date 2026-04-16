from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    db_path: Path
    default_categories: tuple[str, ...]
    github_token: str
    huggingface_token: str
    alphaxiv_token: str
    arxiv_api_min_interval: float
    huggingface_min_interval: float
    github_min_interval: float
    sync_links_concurrency: int


def parse_categories(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ()

    categories: list[str] = []
    seen: set[str] = set()
    for chunk in raw_value.split(","):
        category = chunk.strip()
        if not category or category in seen:
            continue
        seen.add(category)
        categories.append(category)
    return tuple(categories)


def resolve_categories(cli_value: str | None, default_categories: tuple[str, ...]) -> tuple[str, ...]:
    categories = parse_categories(cli_value)
    if categories:
        return categories
    if default_categories:
        return default_categories
    raise ValueError("No arXiv categories configured. Pass --categories or set DEFAULT_CATEGORIES.")


def load_config(project_root: Path | None = None) -> AppConfig:
    load_dotenv()
    root = project_root or Path(__file__).resolve().parents[2]
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    db_path = data_dir / "ghstars.db"

    return AppConfig(
        project_root=root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        db_path=db_path,
        default_categories=parse_categories(os.getenv("DEFAULT_CATEGORIES")),
        github_token=(os.getenv("GITHUB_TOKEN") or "").strip(),
        huggingface_token=(os.getenv("HUGGINGFACE_TOKEN") or "").strip(),
        alphaxiv_token=(os.getenv("ALPHAXIV_TOKEN") or "").strip(),
        arxiv_api_min_interval=_parse_positive_float(os.getenv("ARXIV_API_MIN_INTERVAL"), default=0.5),
        huggingface_min_interval=_parse_positive_float(os.getenv("HUGGINGFACE_MIN_INTERVAL"), default=0.5),
        github_min_interval=_parse_positive_float(os.getenv("GITHUB_MIN_INTERVAL"), default=0.5),
        sync_links_concurrency=_parse_positive_int(os.getenv("SYNC_LINKS_CONCURRENCY"), default=4),
    )


def _parse_positive_float(raw_value: str | None, *, default: float) -> float:
    text = (raw_value or "").strip()
    if not text:
        return default
    try:
        value = float(text)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def _parse_positive_int(raw_value: str | None, *, default: int) -> int:
    text = (raw_value or "").strip()
    if not text:
        return default
    try:
        value = int(text)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value
