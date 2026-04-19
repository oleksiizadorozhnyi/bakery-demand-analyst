PYTHON     := python3.11
VENV       := .venv
PY         := $(VENV)/bin/python
PIP        := $(PY) -m pip
UVICORN    := $(VENV)/bin/uvicorn
PYTEST     := $(VENV)/bin/pytest

API_HOST   ?= 127.0.0.1
API_PORT   ?= 8000
DATE       ?= $(shell date -v-1d +%Y-%m-%d 2>/dev/null || date -d yesterday +%Y-%m-%d)
MODE       ?= synthetic
SEED_FORCE ?=

# ── colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m

.DEFAULT_GOAL := help

# ── help ───────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "$(BOLD)Bakery Demand Analyst$(RESET)"
	@echo ""
	@echo "  $(CYAN)make setup$(RESET)              Create venv and install dependencies"
	@echo "  $(CYAN)make download-data$(RESET)      Download French Bakery CSV via kagglehub"
	@echo "  $(CYAN)make seed$(RESET)               Seed DB  (MODE=synthetic|semi_synthetic)"
	@echo "  $(CYAN)make api$(RESET)                Start FastAPI server (blocking)"
	@echo "  $(CYAN)make run DATE=YYYY-MM-DD$(RESET) Run analytics pipeline for a date"
	@echo "  $(CYAN)make test$(RESET)               Run all tests"
	@echo "  $(CYAN)make start$(RESET)              Full bootstrap: setup → seed → api (background) → run"
	@echo "  $(CYAN)make clean$(RESET)              Remove DB, outputs, and venv"
	@echo ""
	@echo "  Variables (override on CLI):"
	@echo "    MODE=$(MODE)  DATE=$(DATE)"
	@echo "    API_HOST=$(API_HOST)  API_PORT=$(API_PORT)"
	@echo ""

# ── setup ──────────────────────────────────────────────────────────────────
.PHONY: setup
setup: $(VENV)/bin/activate

$(VENV)/bin/activate: requirements.txt pyproject.toml
	@echo "$(BOLD)→ Creating virtual environment …$(RESET)"
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e . -r requirements.txt
	@echo "$(GREEN)✓ venv ready at $(VENV)/$(RESET)"
	@touch $(VENV)/bin/activate

# ── data download ──────────────────────────────────────────────────────────
.PHONY: download-data
download-data: setup
	@echo "$(BOLD)→ Downloading French Bakery dataset …$(RESET)"
	$(PY) scripts/download_data.py

# ── seed ───────────────────────────────────────────────────────────────────
.PHONY: seed
seed: setup
	@echo "$(BOLD)→ Seeding database (mode=$(MODE)) …$(RESET)"
	$(PY) scripts/seed_db.py --mode $(MODE) $(if $(SEED_FORCE),--force,)
	@echo "$(GREEN)✓ Database seeded$(RESET)"

.PHONY: seed-force
seed-force: setup
	$(MAKE) seed SEED_FORCE=1

.PHONY: seed-semi
seed-semi: download-data
	$(MAKE) seed MODE=semi_synthetic

.PHONY: seed-semi-force
seed-semi-force: download-data
	$(MAKE) seed MODE=semi_synthetic SEED_FORCE=1

# ── api ────────────────────────────────────────────────────────────────────
.PHONY: api
api: setup
	@echo "$(BOLD)→ Starting API on http://$(API_HOST):$(API_PORT) …$(RESET)"
	$(UVICORN) bakery_analyst.api.app:app \
		--host $(API_HOST) --port $(API_PORT) --reload

# ── run pipeline ───────────────────────────────────────────────────────────
.PHONY: run
run: setup
	@echo "$(BOLD)→ Running pipeline for $(DATE) …$(RESET)"
	$(PY) main.py --date $(DATE)

.PHONY: run-mock
run-mock: setup
	@echo "$(BOLD)→ Running pipeline (mock LLM) for $(DATE) …$(RESET)"
	USE_MOCK_LLM=true $(PY) main.py --date $(DATE)

# ── test ───────────────────────────────────────────────────────────────────
.PHONY: test
test: setup
	@echo "$(BOLD)→ Running tests …$(RESET)"
	$(PYTEST) tests/ -v

# ── start (full bootstrap in one command) ──────────────────────────────────
# Runs: setup → seed → api in background → wait for it → run pipeline
.PHONY: start
start: setup
	@echo ""
	@echo "$(BOLD)╔══════════════════════════════════════════╗$(RESET)"
	@echo "$(BOLD)║     Bakery Demand Analyst — Full Start   ║$(RESET)"
	@echo "$(BOLD)╚══════════════════════════════════════════╝$(RESET)"
	@echo ""
	@if [ "$(MODE)" = "semi_synthetic" ]; then \
		echo "$(BOLD)[1/4] Downloading data + seeding database (mode=$(MODE)) …$(RESET)"; \
		$(PY) scripts/download_data.py; \
	else \
		echo "$(BOLD)[1/4] Seeding database (mode=$(MODE)) …$(RESET)"; \
	fi
	$(PY) scripts/seed_db.py --mode $(MODE)
	@echo ""
	@echo "$(BOLD)[2/4] Starting API server in background …$(RESET)"
	$(UVICORN) bakery_analyst.api.app:app \
		--host $(API_HOST) --port $(API_PORT) \
		--log-level warning & \
	echo $$! > .api.pid
	@echo "  Waiting for API to become ready …"
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		sleep 1; \
		if $(PY) -c "import urllib.request; urllib.request.urlopen('http://$(API_HOST):$(API_PORT)/health')" 2>/dev/null; then \
			echo "$(GREEN)  API is up$(RESET)"; break; \
		fi; \
		echo "  still waiting ($$i/10) …"; \
	done
	@echo ""
	@echo "$(BOLD)[3/4] Running analytics pipeline for $(DATE) …$(RESET)"
	@echo "  Tip: set CLAUDE_API_KEY and USE_MOCK_LLM=false in .env for a real report."
	$(PY) main.py --date $(DATE)
	@echo ""
	@echo "$(BOLD)[4/4] Outputs$(RESET)"
	@echo "  out/analysis.csv  — metric table"
	@echo "  out/report.md     — generated report"
	@echo ""
	@echo "$(GREEN)✓ Done. API is still running (PID $$(cat .api.pid)).$(RESET)"
	@echo ""
	@printf "  Stop the API server now? [y/N] "; \
	read answer; \
	case "$$answer" in \
		[yY]*) \
			kill $$(cat .api.pid) 2>/dev/null && echo "$(GREEN)✓ API stopped$(RESET)" || echo "  API was not running"; \
			rm -f .api.pid ;; \
		*) \
			echo "  API left running. Stop later with: $(CYAN)make stop-api$(RESET)" ;; \
	esac
	@echo ""

# ── stop background api ────────────────────────────────────────────────────
.PHONY: stop-api
stop-api:
	@if [ -f .api.pid ]; then \
		kill $$(cat .api.pid) 2>/dev/null && echo "$(GREEN)✓ API stopped$(RESET)" || echo "API was not running"; \
		rm -f .api.pid; \
	else \
		echo "No .api.pid file found"; \
	fi

# ── clean ──────────────────────────────────────────────────────────────────
.PHONY: clean
clean: stop-api
	@echo "$(BOLD)→ Cleaning …$(RESET)"
	rm -f bakery.db analysis.csv report.md .api.pid
	rm -rf $(VENV) bakery_analyst.egg-info __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✓ Clean$(RESET)"

.PHONY: clean-db
clean-db:
	rm -f bakery.db
	@echo "$(GREEN)✓ Database removed$(RESET)"
