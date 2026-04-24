# papertorepo

`papertorepo` is a shared workspace service for:

1. syncing arXiv papers into PostgreSQL
2. discovering linked GitHub repositories
3. refreshing repository metadata
4. exporting CSV snapshots

The current system is intentionally simple:

- one PostgreSQL database
- one FastAPI app
- one background worker
- one WebUI
- one flat CLI

## Structure

The backend uses a single installable Python package under `src/`:

```text
src/papertorepo/
  api/
  core/
  db/
  jobs/
  providers/
  services/
  storage/
```

The frontend lives in `frontend/`.
Database migrations stay in the root `alembic/` directory.

## Runtime model

- `sync-papers` stores arXiv results in PostgreSQL
- `find-repos` respects TTL and only re-checks papers that are unknown, missing, expired, or forced
- `refresh-metadata` refreshes dynamic GitHub metadata every run while preserving stable metadata once initialized
- `export` writes CSV snapshots under the runtime data directory and records them in the database

The default queue is serial:

- one worker
- one claimed job at a time
- later jobs wait in queue order

## Quick start

### Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

Open `http://127.0.0.1:8000`.

Run CLI commands against the same workspace:

```bash
docker compose exec app uv run papertorepo jobs
docker compose exec app uv run papertorepo sync-papers --categories cs.CV --month 2026-04
docker compose exec app uv run papertorepo find-repos --categories cs.CV --month 2026-04
docker compose exec app uv run papertorepo refresh-metadata --categories cs.CV --month 2026-04
docker compose exec app uv run papertorepo export --categories cs.CV --month 2026-04 --output cv-2026-04.csv
```

### Local workflow

```bash
uv sync --dev
cp .env.example .env
uv run papertorepo serve
```

Start the worker in another terminal:

```bash
uv run papertorepo worker
```

## Environment

```dotenv
DATABASE_URL=postgresql+psycopg://papertorepo:papertorepo@db:5432/papertorepo
DATA_DIR=data

DEFAULT_CATEGORIES=cs.CV

GITHUB_TOKEN=
HUGGINGFACE_TOKEN=
ALPHAXIV_TOKEN=

SYNC_PAPERS_ARXIV_MIN_INTERVAL=3.0
SYNC_PAPERS_ARXIV_TTL_DAYS=30
SYNC_PAPERS_ARXIV_ID_BATCH_SIZE=100
SYNC_PAPERS_ARXIV_LIST_PAGE_SIZE=2000

FIND_REPOS_LINK_TTL_DAYS=7
FIND_REPOS_HUGGINGFACE_ENABLED=true
FIND_REPOS_ALPHAXIV_ENABLED=true
FIND_REPOS_HUGGINGFACE_MIN_INTERVAL=0.2
FIND_REPOS_ALPHAXIV_MIN_INTERVAL=0.2
FIND_REPOS_WORKER_CONCURRENCY=24
FIND_REPOS_HUGGINGFACE_MAX_CONCURRENT=4
FIND_REPOS_ALPHAXIV_MAX_CONCURRENT=4

REFRESH_METADATA_GITHUB_MIN_INTERVAL=0.2
REFRESH_METADATA_GITHUB_GRAPHQL_BATCH_SIZE=50
REFRESH_METADATA_GITHUB_REST_FALLBACK_MAX_CONCURRENT=2

JOB_QUEUE_WORKER_POLL_SECONDS=1.0
JOB_QUEUE_RUNNING_TIMEOUT_SECONDS=1800
```

## Testing

```bash
uv sync --dev
uv run pytest
cd frontend && npm ci && npm run build
```
