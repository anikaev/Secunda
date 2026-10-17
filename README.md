# Secunda — асинхронный сервис процессинга платежей

Тестовое задание: FastAPI + Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL,
RabbitMQ/FastStream, Alembic и Docker Compose. Python 3.12, зависимости зафиксированы в `uv.lock`.

## Этапы

1. **Каркас (готово).** Настройки, авторизация, health checks, модели
   `payments` и `outbox`, начальная миграция, точки запуска API/consumer и Docker Compose.
2. **Обработка платежей (текущий этап).** POST/GET `/api/v1/payments`,
   транзакционное создание платежа и outbox-события, relay в процессе consumer,
   один обработчик платежа и webhook, очереди retry и DLQ.
3. **Проверка гарантий и документация (после согласования).** Интеграционные тесты
   с PostgreSQL/RabbitMQ, конкурентные запросы, восстановление после сбоев,
   CI и полные примеры API/webhook.

API сохраняет платёж и событие одной транзакцией. Consumer запускает outbox relay,
обрабатывает сообщения из `payments.new` и отправляет результат на webhook.

## Запуск

Нужны Docker Engine/Desktop и Docker Compose v2.

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps -a
docker compose logs -f api consumer
```

`migrate` выполняет `alembic upgrade head` один раз. API и consumer запускаются
после успешной миграции; consumer также ждёт готовности RabbitMQ.
Значения ключей и паролей из `.env.example` предназначены только для локальной разработки.
При изменении паролей с символами, зарезервированными в URL, нужен percent-encoding
в строках подключения в `compose.yaml`.

Проверка API (значение ключа должно совпадать с `API_KEY` в `.env`):

```bash
curl -H 'X-API-Key: local-development-key' http://localhost:8000/health
curl -H 'X-API-Key: local-development-key' http://localhost:8000/health/ready
```

`/health` проверяет работу HTTP-процесса, `/health/ready` — доступность PostgreSQL.
Все HTTP-маршруты требуют `X-API-Key`; неверный или отсутствующий ключ даёт `401`.
Swagger/ReDoc и публичный OpenAPI-маршрут отключены.
RabbitMQ Management: <http://localhost:15672>, логин/пароль — из `.env`.

Остановка с сохранением данных:

```bash
docker compose down
```

## Локальная разработка

Нужны Python 3.12 и [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync --frozen
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn payments_service.main:create_app --factory --reload
```

В другом терминале:

```bash
uv run faststream run payments_service.consumer:app
```

Проверки:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run alembic check
```

Для `alembic check` нужен работающий PostgreSQL с применённой миграцией.

## Структура

```text
src/payments_service/
  api/
  core/
  db/
  services/
  main.py
  consumer.py
  gateway.py
  messaging.py
  outbox.py
  schemas.py
  worker.py
migrations/
tests/
```

`api` содержит HTTP-маршруты, `services` — создание и обработку платежей.
`messaging.py` объявляет очереди и публикацию, `outbox.py` доставляет события,
`worker.py` управляет подтверждением сообщений и повторами при сбоях БД.

## API

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Заказ 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://client.example/webhook"
  }'
```

В примере указан условный адрес webhook. Перед запуском запроса его нужно заменить
на доступный consumer HTTP(S)-адрес.

Ответ — `202 Accepted` с полями `payment_id`, `status`, `created_at`.
Повтор того же запроса с тем же ключом возвращает тот же платёж с актуальным статусом.
Если содержимое отличается, ответ — `409`. Отсутствующий или пустой ключ, неверная
валюта и некорректная сумма дают `422`.

Получение платежа:

```bash
curl -H 'X-API-Key: local-development-key' \
  http://localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000
```

UUID в примере нужно заменить на полученный `payment_id`. Ответ содержит сумму,
валюту, описание, metadata, ключ идемпотентности, URL webhook, статус и даты.
Несуществующий платёж даёт `404`.

Webhook отправляется POST-запросом с `event_id`, `event_type=payment.processed`,
`payment_id`, `amount`, `currency`, `status`, `created_at` и `processed_at`.
Заголовки `X-Webhook-Id` и `Idempotency-Key` содержат стабильный `event_id`.
Успешный ответ — любой `2xx`. Перенаправления не выполняются.

## Как работает обработка

- Сумма — `Decimal` / PostgreSQL `NUMERIC(18, 2)`; JSON API отдаёт её строкой.
  Суммы с ненулевыми знаками после второго десятичного разряда отклоняются без округления.
- UUID для платежей и событий; даты с часовым поясом, в API — UTC.
- Уникальность idempotency key обеспечивает PostgreSQL. Повтор с тем же содержимым
  возвращает тот же платёж; тот же ключ с другим содержимым даёт `409`.
- Платёж и outbox создаются одной транзакцией. Relay публикует persistent-сообщение
  с publisher confirms, затем отмечает событие опубликованным. Доставка — at least once:
  после сбоя между публикацией и commit возможен дубль. Consumer проверяет сохранённый результат.
- Relay работает фоновой задачей в процессе consumer. Отдельный сервис не нужен.
- Один consumer обрабатывает платёж и отправляет webhook. Результат шлюза сохраняется
  до отправки webhook: ошибка уведомления не меняет результат оплаты.
- Шлюз ждёт 2–5 секунд и возвращает `succeeded` с вероятностью 90%, иначе `failed`.
  На время обработки строка платежа блокируется, чтобы дубли не запускали шлюз параллельно.
- Три попытки **всего**, включая первую. Между ними экспоненциальные задержки
  через TTL retry-очереди, затем DLQ. Бизнес-отказ эмулятора — финальный статус
  `failed` с webhook, а не причина повторно проводить оплату.
- При ошибке webhook событие для retry или DLQ сохраняется в outbox в той же транзакции,
  где проверяется текущий результат. ID записи зависит от события и номера попытки:
  повторная доставка старого сообщения не создаёт ещё один повтор.
- Если недоступна сама БД, consumer публикует retry с подтверждением брокера и только
  потом подтверждает исходное сообщение. Если опубликовать тоже не удалось, исходное
  сообщение возвращается в очередь. Отмена задачи не подтверждает сообщение.
- Outbox повторяет публикацию без ограничения числа попыток с задержкой до 30 секунд.
  Недоступность RabbitMQ не должна превращаться в потерю сохранённого платежа.
- Для повторного webhook используется стабильный ID события. Получатель должен уметь
  дедуплицировать уведомления: подтвердить HTTP-доставку и commit в PostgreSQL
  атомарно невозможно.
- Публикация и обработка идут с гарантией at least once. При сбое после внешнего
  действия, но до его фиксации в БД, физическая попытка может повториться.
  Сохранённые outbox-записи пока не удаляются: они также нужны для дедупликации.

## Очереди

Все очереди durable, типа quorum. У consumer одна подписка и prefetch=1.

| Очередь | Назначение |
| --- | --- |
| `payments.new` | обработка платежа и уведомление |
| `payments.retry.2` | TTL 1 секунда, затем возврат в `payments.new` |
| `payments.retry.3` | TTL 2 секунды, затем возврат в `payments.new` |
| `payments.dead` | окончательные ошибки и некорректные сообщения |

Основной exchange — `payments`, dead-letter exchange — `payments.dlx`, оба direct
и durable. Для переноса между очередями включён at-least-once dead-lettering.
Некорректное сообщение сразу отклоняется в DLQ: обработать его повтором невозможно.
После трёх неудачных webhook платёж сохраняет результат оплаты, а поле
`webhook_delivered_at` остаётся пустым.

Настройки `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_RETRY_MAX_DELAY`,
`PUBLISH_TIMEOUT`, `WEBHOOK_TIMEOUT` можно передать через окружение.
По умолчанию это 0.5 секунды, 50 событий, 30 секунд, 5 секунд и 5 секунд.

## Проверка второго этапа

Автоматические unit-тесты проверяют валидацию API, авторизацию, повтор webhook
без повторной оплаты, завершение неудачных попыток, ACK/NACK и сбои публикации outbox.
Дополнительно выполнена проверка с временным PostgreSQL: миграция и `alembic check`,
восемь конкурентных запросов с одним ключом, повторный запрос, конфликт `409`,
обработка платежа и восстановление уведомления после ошибки.

Подписка FastStream проверена через `TestRabbitBroker`. Полный запуск Docker Compose,
интеграционные тесты с настоящим RabbitMQ, сценарии перезапуска и CI — третий этап.

Документация библиотек: [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
[FastStream RabbitMQ](https://faststream.ag2.ai/latest/rabbit/declare/),
[Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
