#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Изолированный проект: порты доступны внутри сети, тома принадлежат тестовому стенду.
# Для одновременного запуска нескольких копий задайте другое имя проекта.
project="${TEST_COMPOSE_PROJECT:-secunda-integration}"
compose=(docker compose -p "$project" -f compose.yaml -f compose.test.yaml)
if [ -n "${TEST_COMPOSE_OVERRIDE:-}" ]; then
  compose+=(-f "$TEST_COMPOSE_OVERRIDE")
fi

cleanup() {
  result=$?
  trap - EXIT
  if [ "$result" -ne 0 ]; then
    "${compose[@]}" ps -a || true
    "${compose[@]}" logs --no-color --tail=150 || true
  fi
  if ! "${compose[@]}" --profile test down --volumes --remove-orphans; then
    result=1
  fi
  exit "$result"
}
trap cleanup EXIT

"${compose[@]}" build api tests
"${compose[@]}" up -d --wait --wait-timeout 120 api consumer webhook
"${compose[@]}" run --rm --no-deps tests ruff check --no-cache .
"${compose[@]}" run --rm --no-deps tests ruff format --check --no-cache .
"${compose[@]}" run --rm --no-deps tests pytest -x -p no:cacheprovider -m 'not integration'
"${compose[@]}" run --rm --no-deps tests alembic check
"${compose[@]}" run --rm --no-deps tests pytest -x -p no:cacheprovider -m integration tests/integration

printf '\nChecking outbox while consumer is stopped\n'
"${compose[@]}" stop consumer
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery seed
"${compose[@]}" up -d consumer
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery verify

printf '\nChecking recovery after RabbitMQ outage\n'
"${compose[@]}" stop rabbitmq
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery seed
"${compose[@]}" up -d --wait --wait-timeout 120 rabbitmq
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery verify

printf '\nChecking unacknowledged redelivery after consumer crash\n'
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery prepare-crash
"${compose[@]}" stop -t 0 consumer
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery release
"${compose[@]}" up -d consumer
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery verify-crash

printf '\nChecking API and consumer recovery after PostgreSQL outage\n'
"${compose[@]}" stop postgres
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery db-down
"${compose[@]}" up -d --wait --wait-timeout 120 postgres
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery healthy
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery verify-crash
"${compose[@]}" run --rm --no-deps tests python -m tests.integration.recovery smoke

printf '\nAll Compose checks passed.\n'
