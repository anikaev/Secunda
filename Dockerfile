FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
# Необязательный корпоративный сертификат CA доступен только во время сборки.
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv sync --native-tls --frozen --no-dev --no-editable \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app app

COPY alembic.ini ./
COPY migrations ./migrations

USER app
EXPOSE 8000

CMD ["uvicorn", "payments_service.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS test
USER root
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv sync --native-tls --frozen --no-editable \
    && mkdir /state \
    && chown app:app /state
COPY --chown=app:app tests ./tests
USER app
CMD ["pytest", "-p", "no:cacheprovider"]

FROM runtime AS production
