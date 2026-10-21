#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

project="${TEST_COMPOSE_PROJECT:-secunda-integration}"
compose=(docker compose -p "$project" -f compose.yaml -f compose.test.yaml)
if [ -n "${TEST_COMPOSE_OVERRIDE:-}" ]; then
  compose+=(-f "$TEST_COMPOSE_OVERRIDE")
fi

run_in_tests() {
  "${compose[@]}" run --rm --no-deps tests "$@"
}

check_recovery() {
  run_in_tests python -m tests.integration.recovery "$1"
}

cleanup() {
  exit_code=$?
  trap - EXIT
  if [ "$exit_code" -ne 0 ]; then
    "${compose[@]}" ps -a || true
    "${compose[@]}" logs --no-color --tail=150 || true
  fi
  if ! "${compose[@]}" --profile test down --volumes --remove-orphans; then
    exit_code=1
  fi
  exit "$exit_code"
}
trap cleanup EXIT

"${compose[@]}" build api tests
"${compose[@]}" up -d --wait --wait-timeout 120 api consumer webhook
run_in_tests ruff check --no-cache .
run_in_tests ruff format --check --no-cache .
run_in_tests pytest -x -p no:cacheprovider -m 'not integration'
run_in_tests alembic check
run_in_tests pytest -x -p no:cacheprovider -m integration tests/integration

printf '\nПроверяем outbox при остановленном consumer\n'
"${compose[@]}" stop consumer
check_recovery create-pending
"${compose[@]}" up -d consumer
check_recovery check-delivery

printf '\nПроверяем восстановление после остановки RabbitMQ\n'
"${compose[@]}" stop rabbitmq
check_recovery create-pending
"${compose[@]}" up -d --wait --wait-timeout 120 rabbitmq
check_recovery check-delivery

printf '\nПроверяем повторную доставку после аварийной остановки consumer\n'
check_recovery prepare-crash
"${compose[@]}" stop -t 0 consumer
check_recovery release-webhook
"${compose[@]}" up -d consumer
check_recovery check-redelivery

printf '\nПроверяем восстановление после остановки PostgreSQL\n'
"${compose[@]}" stop postgres
check_recovery check-db-outage
"${compose[@]}" up -d --wait --wait-timeout 120 postgres
check_recovery check-readiness
check_recovery check-redelivery
check_recovery check-payment

printf '\nВсе проверки через Compose прошли.\n'
