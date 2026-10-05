UV := UV_CACHE_DIR=/tmp/payments-uv UV_PYTHON_INSTALL_DIR=/tmp/payments-python uv
.PHONY: install lint format format-check typecheck security complexity test test-cov check up down migrate
install:
	$(UV) sync --frozen
lint:
	$(UV) run ruff check .
format:
	$(UV) run ruff format .
format-check:
	$(UV) run ruff format --check .
typecheck:
	$(UV) run mypy src
security:
	$(UV) run bandit -q -r src
complexity:
	$(UV) run radon cc src -s -n C
	$(UV) run radon mi src -s
test:
	$(UV) run pytest
test-cov:
	$(UV) run pytest --cov --cov-report=term-missing --cov-report=xml
check: lint format-check typecheck security complexity test-cov
up:
	docker compose up --build -d --wait
down:
	docker compose down
migrate:
	$(UV) run alembic upgrade head
