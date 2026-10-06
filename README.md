# Асинхронный процессинг платежей

Flow: POST → Payment + Transactional Outbox в PostgreSQL → RabbitMQ → consumer →
gateway (2–5 секунд, 90% успеха) → succeeded/failed → HTTP webhook → retry/DLQ.

Стек: Python 3.14, FastAPI/Pydantic v2, SQLAlchemy async/asyncpg, PostgreSQL 18,
Alembic, FastStream/RabbitMQ 4, httpx, uv. Точные зависимости закреплены в `uv.lock`;
Docker использует Python 3.14.8, PostgreSQL 18.6, RabbitMQ 4.3.6 и uv 0.11.6.

## Запуск

Нужны Docker Engine и Compose v2+:

```bash
cp .env.example .env
```

Замените `API_KEY` и пароли в `.env`. Запуск:

```bash
docker compose up --build -d --wait
```

Health:

```bash
curl http://localhost:8000/health
```

[Swagger](http://localhost:8000/docs). `migrate` применяет миграции до запуска API/consumer.
Остановка; данные остаются в volumes:

```bash
docker compose down
```

Переменные в `.env.example`: обязательные `API_KEY`, `DATABASE_URL`,
`RABBITMQ_URL`; `WEBHOOK_TIMEOUT` (секунды), `RETRY_BASE` (backoff, по умолчанию 1 секунда),
`RELAY_INTERVAL` (опрос Outbox); credentials `POSTGRES_*` и `RABBITMQ_DEFAULT_*`.
URL: локально localhost, в Compose — имена сервисов.
`.env` исключён из Git. Пароли в URL требуют URL-encoding; смена credentials
не переинициализирует существующие volumes.

## API

Endpoints `/api/v1/payments` требуют `X-API-Key`, POST — также
`Idempotency-Key` (1–200 символов). Сумма — положительная decimal-строка, максимум
18 цифр до точки и 2 после; JSON float отклоняется. Валюты: RUB, USD, EUR.
`metadata` — JSON-объект с конечными числами; текст должен быть UTF-8 без NUL.

POST:

```bash
export API_KEY=local-development-key-change-me
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount":"199.90",
    "currency":"RUB",
    "description":"Order 42",
    "metadata":{"order_id":42},
    "webhook_url":"https://example.org/payments/webhook"
  }'
```

GET:

```bash
curl http://localhost:8000/api/v1/payments/PAYMENT_UUID \
  -H "X-API-Key: $API_KEY"
```

POST → 202 с `payment_id`, `status`, `created_at`; GET → детали, включая `processed_at`.
Даты — timezone-aware UTC, суммы — строки. Повтор ключа с эквивалентным payload
возвращает тот же платёж и текущий статус; другой payload → 409.
Неизвестный платёж → 404; неверный/отсутствующий API key → 401;
ошибки body/Idempotency-Key → 422.

Webhook содержит `payment_id`, `status`, `amount`, `currency`, `processed_at`.
Его `Idempotency-Key` равен payment UUID для дедупликации получателем.
URL должен быть доступен **из consumer-контейнера**: localhost означает сам контейнер.
Gateway-отказ (`failed`) также доставляется клиенту.

## Гарантии и решения

- Payment и единственное Outbox-событие создаются атомарно. PostgreSQL UNIQUE защищает
  от конкурентных POST; эквивалентность нормализованного payload проверяется через JSONB.
- Relay использует `SKIP LOCKED` и 30-секундный lease: несколько relay допустимы,
  после crash событие снова доступно. Persistent-сообщения идут через durable exchange
  `payments` в `payments.new`; mandatory routing и publisher confirms предшествуют
  записи `published_at`. DB-транзакции не удерживаются во время внешних вызовов/backoff.
- Outbox и webhook — **at-least-once**: crash между отправкой и фиксацией в БД допускает
  дубль. Single-active-consumer и prefetch=1 обеспечивают последовательную обработку.
  Terminal status неизменяем; повторная доставка продолжает только незавершённый webhook.
- Webhook: всего **3 попытки**, backoff 1/2 секунды при timeout, non-2xx или сетевых
  ошибках. Счётчик сохраняется до HTTP и переживает restart; crash может израсходовать
  попытку без доставки. После исчерпания — reject без requeue, DLX `payments.dlx` →
  `payments.dead` → durable `payments.dlq`; terminal payment сохраняется.
  Временные ошибки PostgreSQL повторяются до 3 раз. Неожиданные processing errors
  логируются и также направляются в DLQ.
- При недоступном RabbitMQ API принимает платежи; consumer/relay восстанавливают соединение.
  `/health` проверяет PostgreSQL; SIGTERM закрывает задачи и соединения.

## Разработка и проверки

```bash
make install  # uv sync --frozen; Python 3.14
make migrate  # DATABASE_URL из .env
uv run uvicorn payments.api:create_app --factory --reload
```

Consumer (другой терминал):

```bash
uv run python -m payments.consumer
```

Unit/API tests без инфраструктуры:

```bash
uv run pytest -m 'not integration'
```

Ruff lint/format, strict mypy, Bandit, Radon (complexity A/B,
maintainability A), pytest/coverage >=80%:

```bash
make check
```

Без `TEST_*` integration tests пропускаются. CI проверяет полный gate с PostgreSQL/RabbitMQ и Docker/E2E.
Цели Makefile: `format`, `test`, `test-cov`, `up`, `down`.

<details>
<summary>Ручная настройка integration tests</summary>

**Отдельные** PostgreSQL база (суффикс `_test`) и RabbitMQ vhost.
Тесты очищают таблицы и очереди. Пример с `.env.example`:

```bash
docker compose stop consumer
docker compose exec -T postgres createdb -U payments payments_test
docker compose exec -T rabbitmq rabbitmqctl add_vhost payments_test
docker compose exec -T rabbitmq rabbitmqctl set_permissions -p payments_test payments '.*' '.*' '.*'
export TEST_DATABASE_URL='postgresql+asyncpg://payments:local-development-password@localhost:5432/payments_test'
export TEST_RABBITMQ_URL='amqp://payments:local-development-password@localhost:5672/payments_test'
DATABASE_URL="$TEST_DATABASE_URL" uv run alembic upgrade head
make check
DATABASE_URL="$TEST_DATABASE_URL" uv run alembic check
docker compose start consumer
```

</details>

<details>
<summary>E2E на Docker Compose</summary>

Gateway и HTTP-получатель:

```bash
docker compose cp tools/e2e.py api:/tmp/e2e.py
docker compose exec -T api python /tmp/e2e.py
docker compose stop rabbitmq
docker compose exec -T api python /tmp/e2e.py outage-create
docker compose start rabbitmq
docker compose exec -T api python /tmp/e2e.py outage-check
```

Проверки: полный flow, idempotency/auth, retry/DLQ, duplicate delivery и Outbox recovery.
E2E создаёт диагностические платежи; повторный outage требует чистой среды
из-за фиксированного ключа `outage-e2e`.

</details>

## Ограничения

Gateway — эмулятор; crash до фиксации результата допускает повтор. Реальному gateway
нужен idempotency key, в том числе при сетевых разделениях. Последовательный consumer
ограничивает throughput. DLQ требует мониторинга и ручного replay.
Очистка Outbox, TLS, секреты, метрики и webhook signatures — production hardening.
URL validation **не защищает полностью от SSRF**: нужны ограничения исходящего
трафика/адресов и проверка DNS.
