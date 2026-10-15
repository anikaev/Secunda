# Secunda — асинхронный сервис процессинга платежей

Тестовое задание: FastAPI + Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL,
RabbitMQ/FastStream, Alembic и Docker Compose. Python 3.12, зависимости зафиксированы в `uv.lock`.

## Этапы

1. **Каркас (текущий этап).** Настройки, авторизация, health checks, модели
   `payments` и `outbox`, начальная миграция, точки запуска API/consumer и Docker Compose.
2. **Обработка платежей (после согласования).** POST/GET `/api/v1/payments`,
   транзакционное создание платежа и outbox-события, relay в процессе consumer,
   один обработчик платежа и webhook, очереди retry и DLQ.
3. **Проверка гарантий и документация (после согласования).** Интеграционные тесты
   с PostgreSQL/RabbitMQ, конкурентные запросы, восстановление после сбоев,
   CI и полные примеры API/webhook.

На первом этапе платежные эндпоинты отсутствуют. Consumer запускает соединение
FastStream с RabbitMQ; подписка и relay появятся во втором коммите.

## Запуск каркаса

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
  api/          # маршруты и проверка API-ключа
  core/         # настройки из окружения
  db/           # модели и управление async-сессиями
  main.py       # фабрика FastAPI-приложения
  consumer.py   # FastStream-приложение
migrations/     # Alembic
tests/          # проверки реализованного поведения
```

## Принятые решения для следующих этапов

- Сумма — `Decimal` / PostgreSQL `NUMERIC(18, 2)`; JSON API будет отдавать её строкой.
  Значения с более чем двумя знаками после запятой будут отклоняться, без округления.
- UUID для платежей и событий; даты с часовым поясом, в API — UTC.
- Уникальность idempotency key обеспечивает PostgreSQL. Повтор с тем же содержимым
  вернёт тот же платёж; тот же ключ с другим содержимым даст `409`.
- Платёж и outbox создаются одной транзакцией. Relay публикует persistent-сообщение
  с publisher confirms, затем отмечает событие опубликованным. Доставка — at least once:
  после сбоя между публикацией и commit возможен дубль, consumer должен его выдерживать.
- Relay работает фоновой задачей в процессе consumer. Отдельный сервис не нужен.
- Один consumer обрабатывает платёж и отправляет webhook. Результат шлюза сохраняется
  до отправки webhook: ошибка уведомления не меняет результат оплаты.
- Три попытки **всего**, включая первую. Между ними экспоненциальные задержки
  через TTL retry-очереди, затем DLQ. Бизнес-отказ эмулятора — финальный статус
  `failed` с webhook, а не причина повторно проводить оплату.
- Для повторного webhook будет стабильный ID события. Получатель должен уметь
  дедуплицировать уведомления: подтвердить HTTP-доставку и commit в PostgreSQL
  атомарно невозможно.

Документация библиотек: [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
[FastStream RabbitMQ](https://faststream.ag2.ai/latest/rabbit/declare/),
[Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
