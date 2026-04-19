"""10-step pipeline orchestrator for the Bakery Demand Analyst.

Steps
-----
1.  Call the demand API for the target date.
2.  Handle API errors gracefully (non-fatal: print warning, exit if no data).
3.  Validate and classify prediction records.
4.  Exit early if no valid predictions survive validation.
5.  Print validated prediction summary.
6.  Compute analytics metrics for each shop/product combination.
7.  Build the analysis table.
8.  Build the LLM prompt from the analysis.
9.  Call Claude (or mock) and receive the report.
10. Save analysis.csv and report.md; print terminal summary.
"""

from __future__ import annotations

import sys
from datetime import datetime

import httpx

from bakery_analyst.analysis.service import rows_to_csv, run_analysis
from bakery_analyst.config import settings
from bakery_analyst.models.domain_models import AnalysisRow
from bakery_analyst.reporting.charts import generate_report_charts
from bakery_analyst.reporting.llm_client import generate_report
from bakery_analyst.reporting.prompt_builder import build_prompts
from bakery_analyst.reporting.writer import save_report
from bakery_analyst.repository.demand_repository import fetch_predictions, validate_predictions


def _banner(step: int, text: str) -> None:
    print(f"\n[{step:02d}] {text}")


def _fmt_pct(v: float | None) -> str:
    return f"{v * 100:.1f}%" if v is not None else "n/a"


def _fmt_float(v: float | None, prefix: str = "") -> str:
    if v is None:
        return "n/a"
    sign = "+" if v >= 0 else ""
    return f"{prefix}{sign}{v:.2f}"


def run(
    target_date: str,
    api_base_url: str,
    analysis_path: str = "analysis.csv",
    report_path: str = "report.md",
) -> int:
    """Execute the full pipeline for *target_date*.

    Returns an exit code: 0 = success, 1 = fatal error.
    """
    started_at = datetime.now()
    print(f"\n{'='*60}")
    print(f"  Bakery Demand Analyst — {target_date}")
    print(f"{'='*60}")

    # ------------------------------------------------------------------
    # Step 1 — Fetch predictions from API
    # ------------------------------------------------------------------
    _banner(1, f"Fetching predictions from {api_base_url} …")
    try:
        demand_response = fetch_predictions(api_base_url, target_date, timeout=15.0)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            print(f"  [error] No predictions found for {target_date}. Exiting.")
        else:
            print(f"  [error] API returned HTTP {exc.response.status_code}: {exc.response.text}")
        return 1
    except httpx.RequestError as exc:
        print(f"  [error] Could not reach API: {exc}. Is the server running?")
        return 1

    raw_count = len(demand_response.predictions)
    print(f"  Received {raw_count} raw prediction record(s).")

    # ------------------------------------------------------------------
    # Step 2-3 — Validate and classify predictions
    # ------------------------------------------------------------------
    _banner(2, "Validating and classifying prediction records …")
    validated = validate_predictions(demand_response.predictions)

    n_complete = sum(1 for p in validated if p.prediction_quality == "complete")
    n_partial = len(validated) - n_complete
    dropped = raw_count - len(validated)
    if dropped:
        print(f"  Dropped {dropped} record(s) — missing critical fields.")
    print(f"  Valid: {len(validated)} ({n_complete} complete, {n_partial} partial).")

    # ------------------------------------------------------------------
    # Step 4 — Exit if nothing to analyse
    # ------------------------------------------------------------------
    if not validated:
        print("\n  [fatal] No valid predictions — nothing to analyse. Exiting.")
        return 1

    # ------------------------------------------------------------------
    # Step 5 — Print validated prediction summary
    # ------------------------------------------------------------------
    _banner(3, "Validated predictions:")
    for p in validated:
        q_tag = "complete" if p.prediction_quality == "complete" else "PARTIAL"
        print(f"  {p.shop_id:10s} / {p.product_code:12s}  pred={p.pred_point:6.1f}  [{q_tag}]")

    # ------------------------------------------------------------------
    # Step 6-7 — Compute metrics and build analysis table
    # ------------------------------------------------------------------
    _banner(4, f"Computing metrics (main={settings.main_window_days}d, recent={settings.recent_window_days}d) …")
    rows: list[AnalysisRow] = run_analysis(validated, target_date)

    # ------------------------------------------------------------------
    # Step 7.5 — Generate charts for flagged combinations
    # ------------------------------------------------------------------
    _banner(5, "Generating charts …")
    chart_bundle = generate_report_charts(rows, target_date)
    if chart_bundle is not None:
        charts_generated = ["forecast_vs_actual"]
        if chart_bundle.waste_stockout_bars is not None:
            charts_generated.append("waste_stockout_bars")
        if chart_bundle.temp_scatter is not None:
            charts_generated.append("temp_scatter")
        print(f"  Charts: {', '.join(charts_generated)}")
    else:
        print("  No flags raised — charts skipped.")

    # ------------------------------------------------------------------
    # Step 8 — Build LLM prompts
    # ------------------------------------------------------------------
    _banner(6, "Building report prompt …")
    system_prompt, user_prompt = build_prompts(rows, target_date)
    flagged = sum(
        1 for r in rows
        if any([r.high_waste_flag, r.frequent_stockout_flag,
                r.high_variability_flag, r.persistent_overforecast_flag])
    )
    print(f"  {len(rows)} combinations analysed, {flagged} with risk flags.")

    # ------------------------------------------------------------------
    # Step 9 — Generate report
    # ------------------------------------------------------------------
    llm_label = "mock LLM" if settings.use_mock_llm else f"Claude ({settings.claude_model})"
    _banner(7, f"Generating report via {llm_label} …")
    report_text = generate_report(system_prompt, user_prompt, target_date, len(rows), rows)
    print("  Report generated.")

    # ------------------------------------------------------------------
    # Step 10 — Save outputs
    # ------------------------------------------------------------------
    _banner(8, "Saving outputs …")
    rows_to_csv(rows, analysis_path)
    print(f"  Saved analysis → {analysis_path}")
    save_report(report_text, report_path, chart_bundle=chart_bundle)
    print(f"  Saved report   → {report_path}")

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - started_at).total_seconds()
    print(f"\n{'='*60}")
    print(f"  SUMMARY — {target_date}")
    print(f"{'='*60}")
    for r in rows:
        flags = []
        if r.high_waste_flag:            flags.append("HIGH_WASTE")
        if r.frequent_stockout_flag:     flags.append("STOCKOUT")
        if r.high_variability_flag:      flags.append("HIGH_CV")
        if r.persistent_overforecast_flag: flags.append("OVERFORECAST")
        if r.incomplete_prediction_flag: flags.append("PARTIAL_PRED")
        flag_str = ", ".join(flags) if flags else "—"
        print(
            f"  {r.shop_id:10s} / {r.product_code:12s}"
            f"  bias={_fmt_float(r.mean_signed_error):>8s}"
            f"  waste={_fmt_pct(r.waste_rate):>7s}"
            f"  stockout={_fmt_pct(r.stockout_rate):>7s}"
            f"  [{flag_str}]"
        )
    print(f"\n  Completed in {elapsed:.1f}s.")
    print(f"{'='*60}\n")
    return 0
