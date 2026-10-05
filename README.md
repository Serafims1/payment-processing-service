# Асинхронный процессинг платежей

FastAPI API принимает платёж, PostgreSQL сохраняет Payment и Outbox одной транзакцией.
Relay публикует событие в RabbitMQ; consumer эмулирует gateway (2–5 секунд, 90% успеха),
сохраняет результат и отправляет HTTP webhook.

Стек: Python 3.14, FastAPI/Pydantic v2, SQLAlchemy async/asyncpg, PostgreSQL 18,
Alembic, FastStream/RabbitMQ 4, httpx, uv. Точные зависимости закреплены в `uv.lock`;
Docker использует Python 3.14.8, PostgreSQL 18.6, RabbitMQ 4.3.6 и uv 0.11.6.

## Запуск

Нужны Docker Engine и Compose v2+:

```bash
cp .env.example .env
# Замените API_KEY и локальные пароли в .env.
docker compose up --build -d --wait
curl http://localhost:8000/health
```

Миграции применяет одноразовый сервис `migrate` до запуска API/consumer.
Данные PostgreSQL и RabbitMQ сохраняются в volumes. Остановка без удаления данных:
`docker compose down`. Swagger: http://localhost:8000/docs.

`.env.example` содержит все переменные. `API_KEY`, `DATABASE_URL`, `RABBITMQ_URL`
обязательны; URL при локальном запуске используют localhost, Compose подставляет
имена сервисов. `WEBHOOK_TIMEOUT` — timeout HTTP в секундах, `RETRY_BASE` — основание
backoff (по умолчанию 1 секунда), `RELAY_INTERVAL` — пауза между опросами Outbox.
`POSTGRES_*` и `RABBITMQ_DEFAULT_*` задают credentials контейнеров. Это значения
для разработки; `.env` не хранится в Git. При смене паролей существующие volumes
не переинициализируются. Пароли в connection URL должны быть URL-encoded.

## API

Все `/api/v1/payments` endpoints требуют `X-API-Key`. Для POST также нужен
`Idempotency-Key` (1–200 символов). Сумма — положительная decimal-строка, максимум
18 цифр до точки и 2 после; JSON float отклоняется. Валюты: RUB, USD, EUR.
`metadata` — JSON-объект с конечными числами; текст должен быть UTF-8 без NUL.

```bash
export API_KEY=local-development-key-change-me
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"199.90","currency":"RUB","description":"Order 42","metadata":{"order_id":42},"webhook_url":"https://example.org/payments/webhook"}'

curl http://localhost:8000/api/v1/payments/PAYMENT_UUID \
  -H "X-API-Key: $API_KEY"
```

POST возвращает 202 с `payment_id`, `status`, `created_at`. GET возвращает детали,
включая `processed_at`. Даты timezone-aware UTC, денежные значения в ответах — строки.
Повторный POST с тем же ключом и эквивалентным payload возвращает существующий платёж
и его текущий статус. Другой payload с тем же ключом → 409; отсутствующий платёж → 404;
неверный/отсутствующий API key → 401; ошибки body/Idempotency-Key → 422.

Webhook содержит `payment_id`, `status`, `amount`, `currency`, `processed_at`.
Заголовок `Idempotency-Key` webhook равен payment UUID: получатель должен дедуплицировать
доставки. URL должен быть доступен **из consumer-контейнера**, localhost означает сам
контейнер. Gateway-отказ — нормальный terminal status `failed`, также доставляемый клиенту.

## Гарантии и решения

- Routes отвечают за HTTP; service оркестрирует persistence; SQL находится в repositories.
- PostgreSQL UNIQUE — окончательная защита от конкурентных POST. После IntegrityError
  транзакция откатывается, нормализованный payload сравнивается через JSONB equality
  (boolean отличается от number; порядок ключей и числовой scale не влияют).
  Payment и единственное Outbox-событие создаются атомарно.
- Relay выбирает событие `FOR UPDATE SKIP LOCKED`, фиксирует lease на 30 секунд и завершает
  DB-транзакцию **до** RabbitMQ publish. Durable exchange `payments`, routing key/очередь
  `payments.new`; persistent messages, mandatory routing и publisher confirms.
  `published_at` записывается только после подтверждения. Ошибка освобождает lease;
  crash оставляет событие доступным после истечения lease. Устаревший lease token
  не может завершить новую отправку. Несколько relay допустимы.
- Outbox — at-least-once: crash между publish и DB update допускает дубль. RabbitMQ
  `x-single-active-consumer` и prefetch=1 обеспечивают одного последовательного consumer.
  Terminal status неизменяем; duplicate delivery продолжает только незавершённый webhook.
  Во время gateway, HTTP, backoff и publish нет открытых DB-транзакций.
- Webhook: timeout, non-2xx/network errors, максимум **3 попытки total** с backoff 1/2 секунды.
  Счётчик сохраняется **до** HTTP, поэтому restart не обнуляет бюджет. Crash после резервирования
  попытки может израсходовать её без доставки; это сознательный компромисс строгого лимита.
  После исчерпания consumer rejects без requeue; DLX `payments.dlx` направляет сообщение
  с routing key `payments.dead` в durable `payments.dlq`. Временные ошибки PostgreSQL
  повторяются до 3 раз с backoff; webhook остаётся ограничен своим сохранённым бюджетом.
  Unexpected processing errors также
  попадают в DLQ, логируются и требуют диагностики/replay оператором.
- Webhook — at-least-once, не exactly-once: crash после успешного HTTP и до записи в БД
  допускает повтор. Terminal payment сохраняется даже при неудаче webhook.
- API продолжает принимать платежи при недоступном RabbitMQ; consumer/relay восстанавливают
  соединение. Health API проверяет PostgreSQL. Graceful SIGTERM закрывает задачи и соединения.

## Разработка и проверки

```bash
make install                    # uv sync --frozen; Python 3.14
make migrate                    # DATABASE_URL из .env
uv run uvicorn payments.api:create_app --factory --reload
# Отдельный терминал:
uv run python -m payments.consumer
```

Unit/API tests не требуют инфраструктуры:

```bash
uv run pytest -m 'not integration'
```

Для полного gate нужна **отдельная** PostgreSQL база с именем, заканчивающимся на `_test`,
и отдельный RabbitMQ vhost. Тесты очищают Payment/Outbox и очереди этого vhost.
Не запускайте их против работающего приложения. Например, после остановки consumer:

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

`make check`: Ruff lint/format, strict mypy, Bandit, Radon (complexity A/B,
maintainability A с проверяемым порогом), pytest/coverage >=80%.
`make format`, `make test`, `make test-cov`, `make up/down` доступны отдельно.
Без `TEST_*` integration tests явно skipped; полноценный gate требует обе переменные.
CI запускает их с реальными PostgreSQL/RabbitMQ и отдельно проверяет Docker/E2E.

Воспроизводимый E2E на Compose, с настоящим gateway и локальным HTTP-получателем:

```bash
docker compose cp tools/e2e.py api:/tmp/e2e.py
docker compose exec -T api python /tmp/e2e.py
docker compose stop rabbitmq
docker compose exec -T api python /tmp/e2e.py outage-create
docker compose start rabbitmq
docker compose exec -T api python /tmp/e2e.py outage-check
```

Проверяются POST→Outbox→RabbitMQ→consumer→webhook→GET, idempotency/conflict/auth,
retry, DLQ, duplicate delivery и сохранность Outbox при реальной остановке брокера.
E2E создаёт диагностические платежи; для повторного outage сценария используйте чистую
тестовую среду (фиксированный ключ `outage-e2e`).

## Ограничения

Gateway — эмулятор. Crash до фиксации terminal status может повторить эмуляцию;
реальный gateway обязан принимать idempotency key. Последовательный consumer ограничивает
пропускную способность; RabbitMQ single-active-consumer не заменяет gateway idempotency
при сетевых разделениях. DLQ нужно мониторить и разбирать; автоматического replay нет.
Очистка старых Outbox-событий, TLS, секреты, метрики и webhook signatures — production hardening.
Обычная URL validation **не защищает полностью от SSRF**: перед публичным production
развёртыванием нужны ограничения исходящего трафика/адресов и проверка DNS.
