# Bakery Demand Analyst

A local analytics service that fetches bakery demand predictions from a mock REST API,
computes business metrics from historical SQLite data, generates inline charts for
flagged combinations, and produces a manager-facing report via the Claude API.

---

## What it does

```
Mock API (FastAPI) ──► Prediction fetch & validation
                              │
                    Historical SQLite DB
                              │
                    SQL metric computation
                    (bias, waste, stockouts, variability)
                              │
                    Chart generation (base64 inline PNG)
                              │
                    Claude API prompt
                              │
                    out/analysis_YYYY-MM-DD.csv  +  out/report_YYYY-MM-DD.md
```

Given a target date the pipeline:
1. Fetches demand predictions from a local FastAPI server backed by `forecast_history`.
2. Validates records — drops critical-field failures, flags incomplete quantiles.
3. For each valid shop/product pair, queries 28-day (and 14-day) historical windows.
4. Computes all metrics predominantly in SQL (one documented deviation for Pearson correlation).
5. Applies transparent risk flags against fixed thresholds.
6. Generates inline base64 charts for flagged shop/product combinations.
7. Builds a compact structured prompt and calls Claude (or a mock).
8. Saves `out/analysis_YYYY-MM-DD.csv` and `out/report_YYYY-MM-DD.md`.

---

## Project structure

```
.
├── Makefile                   ← all dev tasks (see below)
├── main.py                    ← CLI entry point: python main.py --date YYYY-MM-DD
├── requirements.txt
├── pyproject.toml
├── .env.example
├── out/                       ← generated outputs (gitignored except .gitkeep)
├── data/
│   └── raw/
│       ├── bakery_sales.csv   ← Kaggle download (semi-synthetic mode only)
│       └── paris_weather.csv  ← auto-fetched and cached by weather_loader
├── scripts/
│   ├── seed_db.py             ← populate the SQLite database
│   ├── run_api.py             ← start the FastAPI server
│   └── download_data.py       ← download French Bakery CSV via kagglehub
├── tests/
│   ├── test_api.py
│   ├── test_seed_sanity.py
│   └── test_semi_seed_sanity.py
└── bakery_analyst/            ← Python package
    ├── config.py              ← pydantic-settings; all env vars
    ├── api/
    │   ├── app.py             ← FastAPI factory
    │   ├── middleware.py      ← failure injection (500s, delays, partial records)
    │   └── routes.py          ← GET /api/demand, GET /health
    ├── db/
    │   ├── connection.py      ← get_connection(), db_session()
    │   ├── schema.py          ← DDL for four tables + indexes
    │   ├── seed.py            ← fully-synthetic data generator
    │   ├── seed_semi.py       ← semi-synthetic seeder (real data + synthetic extensions)
    │   └── loaders/
    │       ├── bakery_loader.py   ← load + aggregate French Bakery CSV
    │       └── weather_loader.py  ← fetch/cache Open-Meteo Paris weather
    ├── models/
    │   ├── api_models.py      ← PredictionRecord, DemandResponse, HealthResponse
    │   └── domain_models.py   ← ValidatedPrediction, AnalysisRow
    ├── repository/
    │   ├── demand_repository.py    ← API fetch + validation/classification
    │   └── analytics_repository.py ← SQL metric queries + correlation helper
    ├── analysis/
    │   └── service.py         ← metric orchestration, risk flags, CSV export
    ├── reporting/
    │   ├── charts.py          ← base64 inline chart generation (matplotlib)
    │   ├── llm_client.py      ← Claude API (real) or mock
    │   ├── prompt_builder.py  ← compact structured prompt construction
    │   └── writer.py          ← report.md writer with embedded charts
    └── pipeline/
        └── runner.py          ← 10-step orchestrator
```

---

## Quick start

### Option A — one command from zero (recommended)

```bash
make start
```

This runs the full bootstrap automatically:
1. Creates `.venv` and installs all dependencies
2. Seeds the database (synthetic mode by default)
3. Starts the API server in the background
4. Runs the analytics pipeline (reads `USE_MOCK_LLM` from `.env`)
5. Prints paths to `out/analysis_YYYY-MM-DD.csv` and `out/report_YYYY-MM-DD.md`
6. Asks whether to stop the API server

With real data and real Claude:

```bash
# copy and edit .env first
cp .env.example .env
# set CLAUDE_API_KEY and USE_MOCK_LLM=false in .env

make start MODE=semi_synthetic DATE=2022-06-15
```

---

### Option B — step by step

#### 1. Create the virtual environment

```bash
make setup
```

Or manually:

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
```

> **Verify before installing:** `which python` must print a path ending in `.venv/bin/python`.
> If it points to a system interpreter, the venv is not active and packages will install in the wrong place.

```bash
python -m pip install -e . -r requirements.txt
```

#### 2. Configure

```bash
cp .env.example .env
# Edit .env — at minimum set CLAUDE_API_KEY or USE_MOCK_LLM=true
```

#### 3. Seed the database

**Synthetic (default) — no external data needed:**

```bash
make seed                        # or: python scripts/seed_db.py
make seed-force                  # wipe and regenerate
```

**Semi-synthetic — real French Bakery demand + real Paris weather:**

Both datasets download automatically:

```bash
make seed-semi                   # downloads data then seeds
make seed-semi-force             # downloads data, wipes DB, reseeds
```

`make seed-semi` runs `scripts/download_data.py` first, which uses
[kagglehub](https://github.com/Kaggle/kagglehub) to fetch the
[French Bakery Daily Sales](https://www.kaggle.com/datasets/matthieugimbert/french-bakery-daily-sales)
dataset. On first run kagglehub opens a browser to log in to Kaggle;
subsequent runs use the cached token. The CSV is saved to
`data/raw/bakery_sales.csv` and skipped on future calls if already present.

Paris weather is fetched from the Open-Meteo Archive API (free, no key)
and cached at `data/raw/paris_weather.csv`. If the network is unavailable,
a synthetic fallback is used with a printed warning.

To download the dataset without seeding:
```bash
make download-data
```

#### 4. Start the API server

```bash
make api                         # foreground, with --reload
# or:
python scripts/run_api.py
```

#### 5. Run the analytics pipeline

```bash
make run DATE=2022-06-15         # real Claude (CLAUDE_API_KEY must be set in .env)
make run-mock DATE=2022-06-15    # mock LLM, no key needed

# or directly:
python main.py --date 2022-06-15
USE_MOCK_LLM=true python main.py --date 2022-06-15

# custom output paths (parent dirs created automatically):
python main.py --date 2022-06-15 --analysis-out custom/metrics.csv --report-out custom/report.md
```

Output files default to:
```
out/analysis_2022-06-15.csv
out/report_2022-06-15.md
```

#### 6. Run tests

```bash
make test                        # or: pytest tests/ -v
```

---

## Makefile reference

```
make setup              Create .venv and install dependencies (idempotent)
make download-data      Download French Bakery CSV via kagglehub (skips if exists)
make seed               Seed DB — synthetic mode (default)
make seed-force         Seed DB — synthetic, wipe first
make seed-semi          Download data + seed DB — semi-synthetic
make seed-semi-force    Download data + wipe DB + reseed — semi-synthetic
make api                Start API server (foreground, with --reload)
make run DATE=…         Run analytics pipeline (real Claude)
make run-mock DATE=…    Run analytics pipeline (mock LLM)
make test               Run all 38 tests
make start              Full bootstrap in one command (see above)
make stop-api           Kill background API process started by make start
make clean              Remove DB, out/ contents, and venv
make clean-db           Remove bakery.db only
```

Override variables on the command line:

```bash
make start MODE=semi_synthetic DATE=2022-07-01 API_PORT=9000
```

| Variable | Default | Description |
|---|---|---|
| `MODE` | `synthetic` | Seed mode: `synthetic` or `semi_synthetic` |
| `DATE` | yesterday | Target date for the pipeline |
| `API_HOST` | `127.0.0.1` | API bind address |
| `API_PORT` | `8000` | API port |
| `SEED_FORCE` | _(unset)_ | Set to any value to force-reseed |

---

## API reference

### `GET /api/demand?date=YYYY-MM-DD`

Returns all `(shop, product)` predictions for the given date from `forecast_history`.

| Status | Condition |
|---|---|
| 200 | Predictions found |
| 404 | No data for that date |
| 422 | Date format invalid or not a real calendar date |

Response shape:
```json
{
  "date": "2022-06-15",
  "predictions": [
    {
      "shop_id": "shop_01",
      "product_code": "croissant",
      "date": "2022-06-15",
      "pred_point": 38.9,
      "pred_q50": 39.7,
      "pred_q80": 42.7,
      "pred_q90": 44.2
    }
  ]
}
```

### `GET /health`

Returns `{"status": "ok", "db_path": "..."}`.

---

## Failure simulation

Set `FAILURE_ENABLED=true` and tune probabilities:

| Variable | Default | Effect |
|---|---|---|
| `FAILURE_ENABLED` | `false` | Master switch |
| `ERROR_500_PROBABILITY` | `0.0` | Fraction of requests returning HTTP 500 |
| `DELAY_PROBABILITY` | `0.0` | Fraction of requests that sleep first |
| `DELAY_SECONDS` | `5.0` | Sleep duration |
| `PARTIAL_RECORD_PROBABILITY` | `0.0` | Per-record probability of dropping quantile fields |

Example — 20 % errors, 30 % partial records:
```bash
FAILURE_ENABLED=true ERROR_500_PROBABILITY=0.2 PARTIAL_RECORD_PROBABILITY=0.3 \
  python scripts/run_api.py
```

---

## Analytics metrics

All metrics are computed from **observed data only** (forecast, sales, waste, and stockout
events). No metric requires unobserved post-stockout demand.

### Forecast error — model quality

| Metric | Source | Description |
|---|---|---|
| `mean_signed_error` | observed | avg(pred_point − units_sold) over 28 days. Positive = systematic over-forecast. |
| `recent_mean_signed_error` | observed | Same formula over the most recent 14 days. Compare absolute magnitudes with `mean_signed_error` to detect whether bias is worsening, stable, or improving. |
| `mae` | observed | avg(&#124;pred_point − units_sold&#124;) — error magnitude regardless of direction. |
| `overforecast_ratio` | observed | Fraction of days where pred_point > units_sold. Shows directional consistency of bias. |

### Waste — operational impact

| Metric | Source | Description |
|---|---|---|
| `waste_rate` | observed | sum(waste_units) / sum(ordered_units). Relative waste burden. |
| `avg_daily_waste_units` | observed | avg(waste_units) per day. **Absolute unit count** — easier to cost out than the rate alone. |

### Stockout — operational risk

| Metric | Source | Description |
|---|---|---|
| `stockout_rate` | observed | Fraction of days in the window with at least one stockout. |
| `stockout_severity_proxy` | **PROXY** | avg(pred_point − ordered_units) on stockout days only. The forecast is used as a proxy for what true demand *may* have been. **Not observed lost demand** — always presented as an estimated gap. Returns `None` if no stockout days exist in the window. |

### Actionability helpers

| Metric | Source | Description |
|---|---|---|
| `bias_adjusted_order` | derived | pred_point − mean_signed_error (28-day bias). Suggested baseline order quantity correcting for observed systematic error. Present as a starting point, not a guarantee. |
| `window_coverage_count` | observed | Number of calendar days in the 28-day window with usable sales data. Values below 20 reduce metric reliability and trigger more cautious report language. |
| `days_since_last_stockout` | observed | Days since the most recent stockout event in the full historical record (not limited to the 28-day window). `None` if no stockout has ever been recorded. |
| `days_since_last_waste` | observed | Days since the most recent day with waste_units > 0 in the full historical record. `None` if no waste has been recorded. |

### Demand variability — internal use

| Metric | Source | Description |
|---|---|---|
| `stddev_units_sold` | observed | Standard deviation of units_sold. Note: censored on stockout days (units_sold = ordered_units), so understates true demand variance when stockout_rate is high. |
| `coefficient_of_variation` | derived | stddev / mean of units_sold. Used as the `high_variability_flag` trigger only; not foregrounded in the client narrative due to censoring and low interpretability. |

### Temperature signal

| Metric | Source | Description |
|---|---|---|
| `temp_sales_correlation` | observed | Pearson r between daily temperature and units_sold. Noisy at 28 days; only surfaced in the report when &#124;r&#124; > 0.35. Never presented as causal. |

### Risk flags

| Flag | Condition |
|---|---|
| `high_waste_flag` | waste_rate > 20 % |
| `frequent_stockout_flag` | stockout_rate > 15 % |
| `high_variability_flag` | CV > 40 % |
| `persistent_overforecast_flag` | overforecast_ratio > 65 % |
| `incomplete_prediction_flag` | prediction had missing quantiles |

### Metric observability key

| Label | Meaning |
|---|---|
| observed | Computed directly from recorded sales, waste, and forecast data |
| derived | Computed from observed metrics (e.g. bias_adjusted_order = pred_point − bias) |
| PROXY | An estimate based on observed data used as a stand-in for an unobservable quantity. Always labelled as such in the report. |

---

## Key design decisions

### Why `forecast_history` was added

Systematic model bias cannot be computed from actual sales alone. You need the
_predicted_ values to compute `mean_signed_error` and `overforecast_ratio`. Without
`forecast_history`, the pipeline would not know whether the model was consistently
over- or under-predicting — only that actual demand varied.

### Why `waste_units` and `ordered_units` were introduced

`waste_units` replaces `planned_waste` because it is _derived_, not estimated:

```
waste_units = max(ordered_units − units_sold, 0)
```

This makes waste causally linked to the supply decision. Without `ordered_units`,
you cannot distinguish:
- "low waste because demand was low" (a good day)
- "low waste because the shop got lucky with its order" (competent ordering)
- "zero waste because the shop stocked out" (demand exceeded supply)

All three look the same if you only have `waste_units` without `ordered_units`.

### Causal data generation structure

For each (date, shop, product):

```
baseline = product_base × shop_multiplier × weekday_factor × oven_factor × weather_factor
actual_demand  ~ Poisson(baseline)                     ← stochastic true demand
ordered_units  = int(baseline × order_bias + noise)    ← separate ordering decision
units_sold     = min(actual_demand, ordered_units)     ← constrained by supply
stockout_flag  = 1 if actual_demand > ordered_units    ← derived, not random
waste_units    = max(ordered_units − units_sold, 0)    ← derived, not random
```

Shops have different ordering biases: shop_01 slightly over-orders (×1.05),
shop_02 under-orders (×0.92), shop_03 over-orders (×1.10). This creates
meaningfully different waste/stockout profiles between shops.

### Seed modes: synthetic vs semi-synthetic

| | Synthetic (default) | Semi-synthetic |
|---|---|---|
| Demand baseline | Parameterised (mean ≈ 22/day croissant, ≈ 38/day baguette) | Real aggregated daily sales from French Bakery CSV |
| Weather | Sinusoidal + noise | Real Paris data via Open-Meteo Archive API, cached locally |
| Shops / supply / forecasts | Synthetic | Synthetic (identical model) |
| External data needed | None | Kaggle CSV download |
| Reproducible offline | Always | Yes (after first fetch, weather is cached) |

**Synthetic mode** uses hard-coded per-product means. Day-to-day variation comes
entirely from the Poisson sampler and weather noise.

**Semi-synthetic mode** replaces those fixed means with actual daily aggregated
quantities from a real Paris bakery. The real time series contains genuine
day-of-week patterns, holiday dips, and seasonal trends without any parameterisation.
The same shop multipliers, ordering policy, and forecast noise are applied on top.

The 90-day window is selected automatically as the contiguous slice with the highest
total volume where both products appear on at least 70 % of days.

### Chart generation

For each report run, `reporting/charts.py` generates up to three inline charts
for the most severely flagged shop/product combination:

- **Forecast vs Actual** — actual sales (solid), point forecast (dashed), Q50–Q90
  prediction band (shaded), stockout markers (red triangles).
- **Waste & Stockout Rates** — grouped bar chart across all combinations; flagged
  bars have a black border; reference lines at the 20 % and 15 % thresholds.
- **Temperature vs Sales scatter** — only generated when |r| > 0.35; includes a
  plain-Python regression line and the r value annotation.

All charts are base64-encoded and embedded directly in `report.md` as inline images —
no separate image files are written to disk.

### SQL-first analytics

All metric computation is in SQL except Pearson correlation, which requires `CORR()`
— not available in SQLite. That deviation is explicitly isolated in
`analytics_repository.compute_temp_sales_correlation()` with a clear inline comment.

### Why SQLite

Zero-setup local portability. The schema and query style are kept close to production
PostgreSQL — parameterised queries, explicit JOIN conditions, no SQLite-only hacks.

---

## Limitations and trade-offs

**Observed sales can be censored by stockouts.**
When `stockout_flag = 1`, `units_sold < actual_demand`. Bias metrics computed against
`units_sold` understate over-forecast errors on those days.

**Correlation is a noisy signal.**
Pearson r over 28 days has high variance. The metric is a data point, not a
conclusion — the report prompt instructs Claude not to over-interpret it.

**No migration system.**
Schema is created idempotently via `CREATE TABLE IF NOT EXISTS`. Dropping and
reseeding is the intended workflow for schema changes.

---

## What would be improved with more time

- **Uncensored demand estimation** — Tobit-style correction or stockout covariate when computing bias.
- **Streaming / incremental pipeline** — maintain a rolling metrics table, append only new dates.
- **Alembic migrations** — production-style schema evolution without data loss.
- **Async repository layer** — replace synchronous SQLite with `aiosqlite` for higher concurrency.
- **Configurable risk thresholds** — move hard-coded constants to `config.py` with env var overrides.

---

## Additional improvements I would consider

- **Delivery layer** — integrate the output into channels like email, Telegram, or dashboards so recommendations reach decision-makers directly.
- **Explainability** — include model explanations (e.g. key drivers like weather, seasonality, weekdays) in the report to make predictions more transparent.
- **Customizable metrics** — allow configuring which metrics are shown depending on business needs.
- **Multi-store support** — extend the system to handle multiple bakery locations and capture correlations between them.
- **Feature enrichment** — incorporate holidays, weekends, and other external signals to improve forecast accuracy and reporting.
- **Human-in-the-loop** — allow users to provide feedback or adjust recommendations, and use that signal to improve future predictions.
- **Audit and usage tracking** — track who views reports and modifies settings to support business workflows.
