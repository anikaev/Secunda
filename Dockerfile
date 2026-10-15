FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app app

COPY alembic.ini ./
COPY migrations ./migrations

USER app
EXPOSE 8000

CMD ["uvicorn", "payments_service.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
