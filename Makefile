.PHONY: help ui-dev ui-build ui-test ui-install api-dev dev test test-ui migrate clean

help:
	@echo "DataSUS ETL — developer commands"
	@echo ""
	@echo "Frontend (requires Bun):"
	@echo "  make ui-install   Install frontend dependencies"
	@echo "  make ui-dev       Run Vite dev server on :5173 (proxies /api to :8787)"
	@echo "  make ui-build     Build the SPA into src/datasus_etl/web/static/"
	@echo "  make ui-test      Run Playwright end-to-end tests"
	@echo ""
	@echo "Backend:"
	@echo "  make api-dev      Run FastAPI + uvicorn with reload on :8787"
	@echo "  make test         Run Python test suite"
	@echo ""
	@echo "Combined:"
	@echo "  make dev          Run backend + frontend in parallel (needs two slots)"

ui-install:
	cd web-ui && bun install

ui-dev:
	cd web-ui && bun run dev

ui-build:
	cd web-ui && bun install && bun run build

ui-test:
	cd web-ui && bun run test:e2e

api-dev:
	uvicorn datasus_etl.web.server:create_app --factory --reload --port 8787

dev:
	@$(MAKE) -j 2 api-dev ui-dev

test:
	python -m pytest tests/ -v

test-ui: ui-build
	python -m pytest tests/ -v
	$(MAKE) ui-test

clean:
	rm -rf src/datasus_etl/web/static
	rm -rf web-ui/node_modules web-ui/dist web-ui/playwright-report web-ui/test-results
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
