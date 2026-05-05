FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json ./package.json
COPY frontend/tsconfig.json ./tsconfig.json
COPY frontend/tsconfig.app.json ./tsconfig.app.json
COPY frontend/tsconfig.node.json ./tsconfig.node.json
COPY frontend/vite.config.ts ./vite.config.ts
COPY frontend/eslint.config.js ./eslint.config.js
COPY frontend/index.html ./index.html
COPY frontend/public ./public
COPY frontend/src ./src
RUN npm install
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime
WORKDIR /app/backend
COPY README.md /app/README.md
COPY .env.example /app/.env.example
COPY backend/pyproject.toml backend/uv.lock backend/main.py backend/alembic.ini ./
COPY backend/alembic ./alembic
COPY backend/src ./src
RUN uv sync --frozen --no-dev
COPY --from=frontend-build /app/frontend/dist /app/frontend/dist
ENV PATH="/app/backend/.venv/bin:${PATH}"
EXPOSE 8000
CMD ["sh", "-lc", "uv run papertorepo migrate && uv run papertorepo serve --host 0.0.0.0 --port 8000"]
