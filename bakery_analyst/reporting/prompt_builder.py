"""Builds system and user prompts for the bakery demand analytics report."""

from __future__ import annotations

from bakery_analyst.config import settings
from bakery_analyst.models.domain_models import AnalysisRow

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a plain-English bakery operations analyst writing a daily report for a \
non-technical store manager. Your job is to translate data into concrete actions \
that can be executed the next morning. Do not sound like a data scientist; write \
like a knowledgeable operations colleague.

**Issue priority order — address issues in this exact order:**
1. Frequent stockouts (lost sales, customer disappointment)
2. Persistent overforecasting (systematic model bias)
3. High waste (over-ordering, margin loss)
4. High demand variability (unpredictable days)
5. Incomplete prediction data (data quality caveat only)

**How to use each metric field:**

- bias_adjusted_order: Use this as the specific order baseline to recommend. \
State it as "consider ordering approximately X units" — it is a starting point \
corrected for observed bias, not a guaranteed figure.

- avg_daily_waste_units: Use the unit count (not just the rate) to make waste \
tangible. "The shop is discarding roughly X units of [product] per day" is more \
actionable than a percentage alone.

- window_coverage_count: If coverage is below 20 days out of the window length \
(default 28), note explicitly that the metrics are based on limited data and \
should be treated with reduced confidence.

- days_since_last_stockout / days_since_last_waste: Use these to distinguish \
active problems from fading ones. A stockout 2 days ago is urgent. One 22 days \
ago may indicate the situation has already improved — acknowledge the uncertainty.

- stockout_severity_proxy: ALWAYS label this as an estimate. It is computed as \
the average gap between the forecast and the order quantity on stockout days — \
not observed lost demand. Use phrasing such as "the forecast suggests the shop \
may have been short by roughly X units on stockout days, though this is an \
estimate based on the model's predictions."

**Temperature correlation rule:**
- If |r| <= 0.35: do NOT mention temperature at all.
- If 0.35 < |r| <= 0.55: mention as a weak, inconclusive signal only — do not \
draw conclusions from it.
- If |r| > 0.55: describe as a moderate trend worth noting, but never as a \
conclusion.
- Never claim temperature "causes" demand changes under any circumstances.

**Conflicting signals rule:**
If a combination has both high waste AND frequent stockouts, acknowledge the \
tension explicitly — over-ordering on some products or days while under-ordering \
on others. Do not pick one signal and ignore the other.

**Evidence calibration — calibrate language to how strong the data is:**
- Strong signal (metric well above threshold, trend consistent): state the \
action directly ("reduce the order by …", "add an extra batch at …").
- Moderate signal (metric near threshold, or short window): use hedged language \
("consider a modest increase", "this may indicate").
- Weak or conflicting signal (near-threshold, short history, or flags pointing \
in opposite directions): name the uncertainty explicitly before any suggestion \
("Only 28 days of data make this inconclusive; if this pattern holds …").
- Never state a cause as fact unless a metric directly implies it.

**Recommendations must be:**
- Operational and grounded in the metrics shown — do not invent causes or \
solutions not supported by the data.
- Name a specific change where the evidence is strong enough to justify one; \
otherwise frame it as something to watch or test.
- Avoid false precision: do not give exact order quantities unless the \
bias_adjusted_order field clearly supports a specific number.

**Required report sections (use this exact order and these exact headers):**

## Executive Summary
Two to three sentences maximum. State the single most urgent issue and one \
positive observation if one exists.

### Shop / Product  ← one section per flagged combination
**Problem:** one sentence describing what the data shows.
**Why it matters:** one sentence on operational impact.
**Action:** one specific, next-day-executable instruction. If bias_adjusted_order \
is available, anchor the recommendation to it.

## What to Monitor Next
Two to four bullet points listing specific metrics or dates to watch.

## Data Quality
Include ONLY if at least one row has prediction_quality == "partial" OR \
window_coverage_count < 20. If all rows are complete and well-covered, omit \
this section entirely.

**Style rules:**
- Maximum 650 words total.
- Use the markdown headers exactly as specified above.
- Do not repeat numbers already in the metrics tables unless needed for emphasis.
"""

# ---------------------------------------------------------------------------
# Flag legend (injected once at the top of the user prompt)
# ---------------------------------------------------------------------------

_FLAG_LEGEND = """\
Flag legend — values that trigger each flag:
  S = stockout_rate > 15%   |  B = overforecast_ratio > 65%
  W = waste_rate > 20%      |  V = CV > 40%
  P = incomplete prediction data

Bias(14d) vs Bias(28d): compare absolute magnitudes to assess trend direction \
(larger absolute value = larger error, regardless of sign).

stockout_severity_proxy is an ESTIMATE based on forecast vs. ordered quantity \
on stockout days — NOT observed lost demand.

bias_adjusted_order = today's pred_point minus 28-day bias. Use as the \
recommended order baseline."""

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _fmt_bias(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"+{value:.1f}" if value >= 0 else f"{value:.1f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_units(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}"


def _fmt_int(value: int | None) -> str:
    if value is None:
        return "n/a"
    return str(value)


def _fmt_days(value: int | None) -> str:
    """Format days-since value as 'Xd' or 'none'."""
    if value is None:
        return "none"
    return f"{value}d"


def _fmt_cv(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _has_flag(row: AnalysisRow) -> bool:
    return (
        row.frequent_stockout_flag
        or row.persistent_overforecast_flag
        or row.high_waste_flag
        or row.high_variability_flag
        or row.incomplete_prediction_flag
    )


def _row_flags(row: AnalysisRow) -> str:
    """Return comma-joined short flag codes in priority order."""
    codes: list[str] = []
    if row.frequent_stockout_flag:
        codes.append("S")
    if row.persistent_overforecast_flag:
        codes.append("B")
    if row.high_waste_flag:
        codes.append("W")
    if row.high_variability_flag:
        codes.append("V")
    if row.incomplete_prediction_flag:
        codes.append("P")
    return ", ".join(codes) if codes else ""


def _signal_strength(value: float, threshold: float) -> str:
    """Classify signal severity relative to the threshold that triggered the flag.

    ratio_above = (value - threshold) / threshold:
      < 30%   → weak   (just over the line)
      30-100% → moderate
      > 100%  → strong (at least 2× threshold)
    """
    if threshold <= 0:
        return "weak"
    ratio_above = (value - threshold) / threshold
    if ratio_above < 0.30:
        return "weak"
    if ratio_above < 1.00:
        return "moderate"
    return "strong"


def _bias_trend(bias_28d: float | None, bias_14d: float | None) -> str:
    """Describe whether recent bias magnitude is worsening, improving, or stable.

    Compares absolute magnitudes so the direction is correct for both positive
    (overforecast) and negative (underforecast) bias values.
    """
    if bias_28d is None or bias_14d is None:
        return "trend unknown (insufficient data)"
    diff = abs(bias_14d) - abs(bias_28d)
    if diff > 0.5:
        return "worsening"
    if diff < -0.5:
        return "improving"
    return "stable"


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def _build_forecast_error_table(rows: list[AnalysisRow]) -> str:
    """Model quality metrics: bias, MAE, overforecast ratio, coverage."""
    header = (
        "| Shop | Product | Bias(28d) | Bias(14d) | MAE | Overforecast% | Coverage | Quality |"
    )
    sep = (
        "|------|---------|-----------|-----------|-----|---------------|----------|---------|"
    )
    lines = [header, sep]
    window = settings.main_window_days
    for row in rows:
        cov = (
            f"{row.window_coverage_count}/{window}"
            if row.window_coverage_count is not None
            else "n/a"
        )
        lines.append(
            f"| {row.shop_id}"
            f" | {row.product_code}"
            f" | {_fmt_bias(row.mean_signed_error)}"
            f" | {_fmt_bias(row.recent_mean_signed_error)}"
            f" | {_fmt_units(row.mae)}"
            f" | {_fmt_pct(row.overforecast_ratio)}"
            f" | {cov}"
            f" | {row.prediction_quality} |"
        )
    return "\n".join(lines)


def _build_operational_table(rows: list[AnalysisRow]) -> str:
    """Operational impact metrics: waste, stockout, recency, order suggestion."""
    header = (
        "| Shop | Product | Waste% | Waste(u/d) | Stockout% | SO Gap† | LastSO | LastW | Order↓ | Flags |"
    )
    sep = (
        "|------|---------|--------|------------|-----------|---------|--------|-------|--------|-------|"
    )
    lines = [header, sep]
    for row in rows:
        lines.append(
            f"| {row.shop_id}"
            f" | {row.product_code}"
            f" | {_fmt_pct(row.waste_rate)}"
            f" | {_fmt_units(row.avg_daily_waste_units)}"
            f" | {_fmt_pct(row.stockout_rate)}"
            f" | {_fmt_units(row.stockout_severity_proxy)}"
            f" | {_fmt_days(row.days_since_last_stockout)}"
            f" | {_fmt_days(row.days_since_last_waste)}"
            f" | {_fmt_units(row.bias_adjusted_order)}"
            f" | {_row_flags(row)} |"
        )
    lines.append("")
    lines.append(
        "† SO Gap = stockout severity proxy (ESTIMATE only — avg forecast minus ordered units "
        "on stockout days; not observed lost demand)."
    )
    lines.append("  Order↓ = bias-adjusted order (pred_point − 28d bias).")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Flagged detail block
# ---------------------------------------------------------------------------

def _flagged_detail_block(row: AnalysisRow) -> str:
    """Build the structured detail block for a single flagged combination."""
    flags = _row_flags(row)
    lines: list[str] = [f"**{row.shop_id} / {row.product_code}** [flags: {flags}]"]

    # Signal strength for stockout and overforecast
    signal_parts: list[str] = []
    if row.frequent_stockout_flag and row.stockout_rate is not None:
        strength = _signal_strength(row.stockout_rate, threshold=0.15)
        signal_parts.append(f"stockout_rate={row.stockout_rate * 100:.1f}% ({strength})")
    if row.persistent_overforecast_flag and row.overforecast_ratio is not None:
        strength = _signal_strength(row.overforecast_ratio, threshold=0.65)
        signal_parts.append(f"overforecast_ratio={row.overforecast_ratio * 100:.0f}% ({strength})")
    if signal_parts:
        lines.append(f"- Signal strength: {', '.join(signal_parts)}")

    # Bias trend
    trend = _bias_trend(row.mean_signed_error, row.recent_mean_signed_error)
    lines.append(
        f"- Bias trend: 28d={_fmt_bias(row.mean_signed_error)}, "
        f"14d={_fmt_bias(row.recent_mean_signed_error)} → {trend}"
    )

    # Bias-adjusted order
    if row.bias_adjusted_order is not None:
        lines.append(
            f"- Bias-adjusted order suggestion: {row.bias_adjusted_order:.1f} units "
            f"(today's forecast minus 28d bias)"
        )

    # Waste volume
    if row.avg_daily_waste_units is not None:
        lines.append(
            f"- Avg daily waste: {row.avg_daily_waste_units:.1f} units/day "
            f"(waste_rate={_fmt_pct(row.waste_rate)})"
        )

    # Stockout severity proxy
    if row.stockout_severity_proxy is not None:
        lines.append(
            f"- Stockout severity (ESTIMATE): avg forecast−order gap on stockout days = "
            f"+{row.stockout_severity_proxy:.1f} units "
            f"[proxy only — not observed lost demand]"
        )

    # Recency
    recency_parts: list[str] = []
    if row.days_since_last_stockout is not None:
        recency_parts.append(f"last stockout {row.days_since_last_stockout}d ago")
    else:
        recency_parts.append("no stockout recorded")
    if row.days_since_last_waste is not None:
        recency_parts.append(f"last waste {row.days_since_last_waste}d ago")
    else:
        recency_parts.append("no waste recorded")
    lines.append(f"- Recency: {', '.join(recency_parts)}")

    # Window coverage
    window = settings.main_window_days
    if row.window_coverage_count is not None:
        cov_note = (
            " ⚠ limited data — treat metrics with caution"
            if row.window_coverage_count < 20
            else ""
        )
        lines.append(
            f"- Window coverage: {row.window_coverage_count}/{window} days with data{cov_note}"
        )

    # Temperature correlation
    r = row.temp_sales_correlation
    if r is not None and abs(r) > 0.35:
        temp_label = (
            "weak, inconclusive — treat with caution"
            if abs(r) <= 0.55
            else "moderate trend worth noting (not a conclusion)"
        )
        lines.append(f"- Temperature correlation: r={r:+.2f} → {temp_label}")

    return "\n".join(lines)


def _build_flagged_detail_section(rows: list[AnalysisRow]) -> str:
    flagged = [r for r in rows if _has_flag(r)]
    if not flagged:
        return ""
    return "\n\n".join(_flagged_detail_block(r) for r in flagged)


def _build_data_quality_note(rows: list[AnalysisRow]) -> str | None:
    partial_rows = [r for r in rows if r.prediction_quality == "partial"]
    low_cov_rows = [
        r for r in rows
        if r.window_coverage_count is not None and r.window_coverage_count < 20
    ]
    if not partial_rows and not low_cov_rows:
        return None

    parts: list[str] = []
    if partial_rows:
        n, m = len(partial_rows), len(rows)
        parts.append(
            f"{n} of {m} combinations have incomplete prediction data (missing quantiles). "
            "Bias and overforecast metrics for these rows may be less reliable."
        )
    if low_cov_rows:
        names = ", ".join(f"{r.shop_id}/{r.product_code}" for r in low_cov_rows)
        parts.append(
            f"Low window coverage (<20/{settings.main_window_days} days): {names}. "
            "All metrics for these combinations have reduced statistical reliability."
        )
    return "\n".join(parts)


def _build_instructions(target_date: str) -> str:
    """Concise trigger block — full rules live in the system prompt."""
    return (
        f"Write the report for {target_date} following the required section structure "
        "from your instructions exactly. Apply all evidence-calibration, temperature, "
        "and conflicting-signals rules as specified above."
    )


# ---------------------------------------------------------------------------
# User prompt assembly
# ---------------------------------------------------------------------------

def _build_user_prompt(rows: list[AnalysisRow], target_date: str) -> str:
    parts: list[str] = []

    # Flag legend and metric notes
    parts.append(_FLAG_LEGEND)

    # Table 1 — Forecast error / model quality
    parts.append(
        "### Table 1 — Forecast Error (model quality)\n\n"
        + _build_forecast_error_table(rows)
    )

    # Table 2 — Operational impact
    parts.append(
        "### Table 2 — Operational Metrics\n\n"
        + _build_operational_table(rows)
    )

    # Section 3 — Flagged combinations detail
    flagged_detail = _build_flagged_detail_section(rows)
    if flagged_detail:
        parts.append("### Section 3 — Flagged Combinations Detail\n\n" + flagged_detail)
    else:
        parts.append(
            "### Section 3 — Flagged Combinations Detail\n\n"
            "No combinations have active flags."
        )

    # Section 4 — Data quality note (conditional)
    quality_note = _build_data_quality_note(rows)
    if quality_note is not None:
        parts.append("### Section 4 — Data Quality Note\n\n" + quality_note)

    # Section 5 — Instructions
    parts.append("### Section 5 — Instructions\n\n" + _build_instructions(target_date))

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_prompts(rows: list[AnalysisRow], target_date: str) -> tuple[str, str]:
    """Return (system_prompt, user_prompt).

    Args:
        rows: List of AnalysisRow objects containing computed analytics for
              each shop/product combination on the target date.
        target_date: ISO-format date string (e.g. "2026-04-17") for which the
                     report is being generated.

    Returns:
        A two-element tuple ``(system_prompt, user_prompt)`` ready to be sent
        to the language model.
    """
    return _SYSTEM_PROMPT, _build_user_prompt(rows, target_date)
