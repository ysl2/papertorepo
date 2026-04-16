from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile

from src.ghstars.models import RawCacheEntry


class RawCacheStore:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_body(
        self,
        *,
        provider: str,
        surface: str,
        request_key: str,
        body: str,
        content_type: str | None,
    ) -> tuple[Path, str]:
        content_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        path = self._build_body_path(
            provider=provider,
            surface=surface,
            request_key=request_key,
            content_hash=content_hash,
            content_type=content_type,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return path, content_hash

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = Path(handle.name)
        try:
            os.replace(temp_path, path)
        except FileNotFoundError:
            pass
        finally:
            if temp_path.exists():
                temp_path.unlink()
        return path, content_hash

    def read_body(self, entry: RawCacheEntry) -> str | None:
        if not entry.body_path.exists():
            return None
        return entry.body_path.read_text(encoding="utf-8")

    def _build_body_path(
        self,
        *,
        provider: str,
        surface: str,
        request_key: str,
        content_hash: str,
        content_type: str | None,
    ) -> Path:
        request_hash = hashlib.sha256(request_key.encode("utf-8")).hexdigest()[:16]
        extension = _extension_for_content_type(content_type)
        file_name = f"{request_hash}-{content_hash[:16]}{extension}"
        return self.root / provider / surface / file_name


def _extension_for_content_type(content_type: str | None) -> str:
    value = (content_type or "").lower()
    if "json" in value:
        return ".json"
    if "xml" in value:
        return ".xml"
    if "html" in value:
        return ".html"
    return ".txt"
