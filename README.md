# scripts.ghstars-ng

Local-first toolkit for building and auditing deterministic arXiv ↔ GitHub associations.

## Scope

- user-configurable arXiv categories
- local SQLite storage for canonical paper facts and link evidence
- deterministic exact-match repo discovery from arXiv, Hugging Face, and AlphaXiv
- multi-process-safe local execution on a single machine
- raw response caching for replay and debugging
- timestamped CSV exports with `published_at`

## Install

```bash
uv sync
cp .env.example .env
```

Adjust `.env` as needed.

## Commands

```bash
uv run main.py sync arxiv --categories cs.CV --month 2026-01
uv run main.py sync links --categories cs.CV --month 2026-01
uv run main.py audit parity --categories cs.CV --month 2026-01
uv run main.py enrich repos --categories cs.CV --month 2026-01
uv run main.py export csv --categories cs.CV --month 2026-01 --output output/papers.csv
```

Supported window filters:

- `--day YYYY-MM-DD`
- `--month YYYY-MM`
- `--from YYYY-MM-DD --to YYYY-MM-DD`

`export csv --output output/papers.csv` writes to a timestamped file such as `output/papers-20260416-071922-151537.csv`.

## Storage

- SQLite database: `data/ghstars.db`
- raw payload cache: `data/raw/`

## Design

The project treats canonical arXiv papers as the primary identity. Provider-specific observations are stored first, then resolved conservatively into final paper-repo links. Repo metadata enrichment and CSV export are downstream steps driven from the local database.
