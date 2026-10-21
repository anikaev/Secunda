# Secunda

Асинхронный сервис обработки платежей. API сохраняет платёж, consumer проводит
его через эмулятор шлюза и отправляет результат на webhook.

Стек: Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL,
RabbitMQ, FastStream и Alembic. Версии зависимостей зафиксированы в `uv.lock`.

## Запуск

Нужны Docker и Docker Compose с поддержкой `!reset` в YAML.

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps -a
docker compose logs -f api consumer
```

Сервис `migrate` применит миграции. После него запустятся API и consumer.
Consumer также дождётся готовности RabbitMQ.

API доступно на `http://localhost:8000`, RabbitMQ Management — на
`http://localhost:15672`. Ключ API и пароли берутся из `.env`.
В `.env.example` стоят значения для локальной разработки.
Если меняете пароль, зарезервированные в URL символы нужно кодировать
в строках подключения в `compose.yaml`.

Проверить API:

```bash
curl -H 'X-API-Key: local-development-key' http://localhost:8000/health
curl -H 'X-API-Key: local-development-key' http://localhost:8000/health/ready
```

`/health` проверяет HTTP-процесс, `/health/ready` — подключение к PostgreSQL.
Все маршруты требуют `X-API-Key`. Без ключа или с неверным ключом вернётся `401`.
Публичные Swagger, ReDoc и OpenAPI отключены.

Остановить сервисы, сохранив данные:

```bash
docker compose down
```

## API

Создать платёж:

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

Замените `webhook_url` на HTTP(S)-адрес, доступный из контейнера consumer.
Адрес из примера условный. `localhost` внутри контейнера указывает на сам контейнер.

Ответ — `202 Accepted` с `payment_id`, `status` и `created_at`.
Сумма должна быть положительной, не больше `9999999999999999.99`,
с точностью до двух знаков после запятой. Валюты: `RUB`, `USD`, `EUR`.
`metadata` — JSON-объект, по умолчанию пустой.

Повтор с тем же `Idempotency-Key` и тем же содержимым вернёт тот же платёж
с его текущим статусом. Если содержимое изменилось, вернётся `409`.
Пустой или отсутствующий ключ и неверные поля запроса дают `422`.

Получить платёж:

```bash
curl -H 'X-API-Key: local-development-key' \
  http://localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000
```

Вместо нулевого UUID подставьте `payment_id` из ответа на POST.
GET вернёт сумму, валюту, описание, metadata, ключ идемпотентности, URL webhook,
статус и даты. Для неизвестного UUID вернётся `404`.
Суммы в JSON отдаются строками, даты — в UTC.

Webhook отправляется POST-запросом с полями `event_id`, `event_type`, `payment_id`,
`amount`, `currency`, `status`, `created_at` и `processed_at`.
`event_type` всегда равен `payment.processed`.
Заголовки `X-Webhook-Id` и `Idempotency-Key` содержат `event_id`.
Сервис считает доставку успешной при любом ответе `2xx` и не переходит по редиректам.

## Обработка и повторы

1. API создаёт платёж со статусом `pending` и запись в `outbox` одной транзакцией.
   Уникальный индекс по ключу идемпотентности защищает от одновременных дублей.
2. Фоновая задача в consumer забирает записи outbox через `FOR UPDATE SKIP LOCKED`.
   Она публикует persistent-сообщение и ждёт подтверждения RabbitMQ, после чего
   отмечает запись опубликованной. Если брокер недоступен, повторяет без ограничения
   числа попыток, увеличивая задержку до 30 секунд.
3. Consumer блокирует строку платежа и вызывает эмулятор шлюза: задержка 2–5 секунд,
   90% ответов `succeeded`, 10% — `failed`. Результат сохраняется до отправки webhook.
   Ошибка уведомления не приводит к повторной оплате. Отказ шлюза — окончательный результат.
4. При ошибке webhook consumer записывает событие повтора в outbox. Его ID зависит
   от исходного события и номера попытки, поэтому повторная доставка старого сообщения
   не создаёт новый повтор. Всего три попытки: первая сразу, следующие через 1 и 2 секунды.
   После третьей ошибки событие отправляется в `payments.dead`.
5. Если сама БД недоступна, consumer публикует повтор напрямую и ждёт подтверждения
   брокера. Только затем подтверждает исходное сообщение. Если публикация не удалась,
   возвращает исходное сообщение в очередь.

Успешный webhook отмечается в `webhook_delivered_at`. После исчерпания попыток
это поле остаётся пустым, а статус платежа сохраняет результат оплаты.
Некорректные сообщения сразу отклоняются в DLQ.

Доставка — **at least once**. При сбое после публикации в RabbitMQ, но до commit
запись outbox будет отправлена повторно. Consumer проверяет сохранённый результат.
Если consumer завершится после ответа шлюза, но до сохранения результата,
вызов шлюза может повториться.
Если сбой произошёл после получения webhook клиентом, но до commit в БД,
уведомление тоже может повториться. Получателю нужно убирать дубли по `event_id`:
он и содержимое webhook не меняются между попытками.
Consumer использует записи outbox для проверки обработанных попыток,
поэтому они пока не удаляются.

## Очереди

У consumer одна подписка и `prefetch=1`. Все очереди durable, типа quorum.

| Очередь | Назначение |
| --- | --- |
| `payments.new` | Обработка платежа и отправка webhook |
| `payments.retry.2` | Ожидание 1 секунду, затем возврат в `payments.new` |
| `payments.retry.3` | Ожидание 2 секунды, затем возврат в `payments.new` |
| `payments.dead` | Окончательные ошибки и некорректные сообщения |

Основной exchange — `payments`, dead-letter exchange — `payments.dlx`.
Оба direct и durable. Между очередями включён at-least-once dead-lettering.
Стандартный лимит аварийных повторных доставок RabbitMQ действует отдельно
от трёх попыток приложения.

## Разработка

Нужны Python 3.12 и [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync --frozen
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn payments_service.main:create_app --factory --reload
```

Consumer запускается в другом терминале:

```bash
uv run faststream run payments_service.consumer:app
```

В `src/payments_service` лежат:

- `api/` — маршруты и проверка API-ключа;
- `db/` — модели и сессии;
- `services/` — создание и обработка платежей;
- `config.py` — настройки из окружения;
- `schemas.py` — запросы, ответы и события;
- `messaging.py` — очереди и публикация;
- `outbox.py` — доставка событий из БД;
- `worker.py` — подтверждение сообщений и повторы при сбоях БД;
- `main.py` и `consumer.py` — запуск API и consumer;
- `gateway.py` — эмулятор платёжного шлюза.

Настройки таймаутов и outbox можно передать через окружение:

| Переменная | По умолчанию |
| --- | --- |
| `OUTBOX_POLL_INTERVAL` | 0.5 секунды |
| `OUTBOX_BATCH_SIZE` | 50 событий за проход |
| `OUTBOX_RETRY_MAX_DELAY` | 30 секунд |
| `PUBLISH_TIMEOUT` | 5 секунд |
| `WEBHOOK_TIMEOUT` | 5 секунд |

## Проверки

Быстрые проверки без запущенных сервисов:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

Проверить, что модели совпадают с миграциями, при работающем PostgreSQL:

```bash
uv run alembic check
```

Полный прогон через Docker:

```bash
bash scripts/test-compose.sh
```

Скрипт сам собирает образы, поднимает PostgreSQL, RabbitMQ, API, consumer
и тестовый получатель webhook. Проверяет Ruff, unit-тесты, миграции и интеграцию:
одновременные запросы с одним ключом, дубли AMQP, задержки retry, DLQ,
откат транзакции и сбой commit после подтверждённой публикации.
Затем проверяет восстановление после остановки consumer, RabbitMQ, PostgreSQL
и аварийного завершения consumer во время webhook.

Нужны только Docker, Compose и Bash. Тестовый стенд задаёт свои настройки,
поэтому файл `.env` для него не нужен. Порты хоста он не занимает.
Эмулятор шлюза остаётся настоящим:
тесты ждут 2–5 секунд и принимают оба финальных статуса.
Получатель webhook позволяет задавать HTTP-ответы и удерживать запрос.
В рабочий образ он не входит.

По завершении скрипт удаляет свои контейнеры и тома. При ошибке печатает логи
и возвращает ненулевой код. Для параллельных запусков задайте разные имена проекта:

```bash
TEST_COMPOSE_PROJECT=secunda-check-2 bash scripts/test-compose.sh
```

Тот же прогон запускается в [GitHub Actions](https://github.com/anikaev/Secunda/actions/workflows/ci.yaml)
для pull request и при push в `main`. Его также можно запустить вручную.

Если зависимости нужно собирать за корпоративным TLS-прокси, Dockerfile принимает
BuildKit secret `build_ca` с PEM-сертификатами. Подключить его можно через локальный
Compose override в `TEST_COMPOSE_OVERRIDE`. Сертификат доступен только во время сборки.
