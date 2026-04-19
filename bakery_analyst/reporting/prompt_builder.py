"""Builds system and user prompts for the bakery demand analytics report."""

from __future__ import annotations

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

**Temperature correlation rule:**
- If |r| <= 0.35: do NOT mention temperature at all.
- If 0.35 < |r| <= 0.55: you may mention temperature as a weak, inconclusive \
signal only — do not draw conclusions from it.
- If |r| > 0.55: you may describe temperature as a moderate trend worth noting, \
but never as a conclusion.
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
- Never state a cause as fact unless a metric directly implies it. Use \
"this may indicate", "a possible cause is", or "one explanation could be".

**Recommendations must be:**
- Operational and grounded in the metrics shown — do not invent causes or \
solutions not supported by the data
- Name a specific change where the evidence is strong enough to justify one; \
otherwise frame it as something to watch or test
- Avoid false precision: do not give exact order quantities unless the bias \
metric clearly supports a specific number

**Required report sections (use this exact order and these exact headers):**

## Executive Summary
Two to three sentences maximum. State the single most urgent issue and one \
positive observation if one exists.

### Shop / Product  ← one section per flagged combination
**Problem:** one sentence describing what the data shows.
**Why it matters:** one sentence on operational impact (lost sales, waste cost, etc.).
**Action:** one specific, next-day-executable instruction.

## What to Monitor Next
Two to four bullet points listing specific metrics or dates to watch.

## Data Quality
Include this section ONLY if at least one row has prediction_quality == "partial". \
If all rows are complete, omit this section entirely.

**Style rules:**
- Maximum 600 words total.
- Use the markdown headers exactly as specified above.
- Do not repeat numbers already in the metrics table unless needed for emphasis \
on a particularly severe value.
"""

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _fmt_bias(value: float | None) -> str:
    """Format mean_signed_error as '+X.X' or '-X.X', or 'n/a' when absent."""
    if value is None:
        return "n/a"
    return f"+{value:.1f}" if value >= 0 else f"{value:.1f}"


def _fmt_pct(value: float | None) -> str:
    """Format a 0-1 ratio as 'X.X%', or 'n/a' when absent."""
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_cv(value: float | None) -> str:
    """Format coefficient_of_variation as 'X.XX', or 'n/a' when absent."""
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _has_flag(row: AnalysisRow) -> bool:
    """Return True if any risk flag is set on the row."""
    return (
        row.frequent_stockout_flag
        or row.persistent_overforecast_flag
        or row.high_waste_flag
        or row.high_variability_flag
        or row.incomplete_prediction_flag
    )


def _row_flags(row: AnalysisRow) -> str:
    """Return comma-joined short flag codes in priority order.

    Priority: S (stockout) > B (overforecast) > W (waste) > V (variability) > P (partial).
    """
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


def _signal_strength(ratio: float) -> str:
    """Classify a ratio (0-1) into weak / moderate / strong signal label."""
    pct = ratio * 100
    if pct < 25:
        return "weak"
    if pct <= 50:
        return "moderate"
    return "strong"


def _bias_trend(bias_28d: float | None, bias_14d: float | None) -> str:
    """Describe whether recent bias is worsening, improving, or stable.

    Worsening if recent > 28d by >0.5 (absolute values compared for direction).
    Improving if recent < 28d by >0.5.
    Otherwise stable.
    """
    if bias_28d is None or bias_14d is None:
        return "trend unknown (insufficient data)"
    diff = bias_14d - bias_28d
    if diff > 0.5:
        return "worsening"
    if diff < -0.5:
        return "improving"
    return "stable"


def _flagged_detail_block(row: AnalysisRow) -> str:
    """Build the structured detail block for a single flagged combination."""
    flags = _row_flags(row)
    lines: list[str] = [f"**{row.shop_id} / {row.product_code}** [flags: {flags}]"]

    # Signal strength lines
    signal_parts: list[str] = []
    if row.frequent_stockout_flag and row.stockout_rate is not None:
        strength = _signal_strength(row.stockout_rate)
        signal_parts.append(
            f"stockout_rate={row.stockout_rate * 100:.1f}% ({strength})"
        )
    if row.persistent_overforecast_flag and row.overforecast_ratio is not None:
        strength = _signal_strength(row.overforecast_ratio)
        signal_parts.append(
            f"overforecast_ratio={row.overforecast_ratio * 100:.0f}% ({strength})"
        )
    if signal_parts:
        lines.append(f"- Signal strength: {', '.join(signal_parts)}")

    # Bias trend
    if row.mean_signed_error is not None or row.recent_mean_signed_error is not None:
        trend = _bias_trend(row.mean_signed_error, row.recent_mean_signed_error)
        bias_28d_str = _fmt_bias(row.mean_signed_error)
        bias_14d_str = _fmt_bias(row.recent_mean_signed_error)
        lines.append(
            f"- Bias trend: 28d bias={bias_28d_str}, recent 14d bias={bias_14d_str}"
            f" \u2192 bias {trend}"
        )

    # Temperature correlation — only show when |r| > 0.35
    r = row.temp_sales_correlation
    if r is not None and abs(r) > 0.35:
        if abs(r) <= 0.55:
            temp_label = "weak, inconclusive signal — treat with caution"
        else:
            temp_label = "moderate trend worth noting (not a conclusion)"
        lines.append(f"- Temperature correlation: r={r:+.2f} \u2192 {temp_label}")

    return "\n".join(lines)


def _build_metrics_table(rows: list[AnalysisRow]) -> str:
    """Build the compact metrics markdown table."""
    header = (
        "| Shop | Product | Bias(28d) | Waste% | Stockout% | CV | Flags | Quality |"
    )
    separator = (
        "|------|---------|-----------|--------|-----------|------|-------|---------|"
    )
    table_lines = [header, separator]
    for row in rows:
        table_lines.append(
            f"| {row.shop_id}"
            f" | {row.product_code}"
            f" | {_fmt_bias(row.mean_signed_error)}"
            f" | {_fmt_pct(row.waste_rate)}"
            f" | {_fmt_pct(row.stockout_rate)}"
            f" | {_fmt_cv(row.coefficient_of_variation)}"
            f" | {_row_flags(row)}"
            f" | {row.prediction_quality} |"
        )
    return "\n".join(table_lines)


def _build_flagged_detail_section(rows: list[AnalysisRow]) -> str:
    """Build Section 2: structured detail blocks for all flagged combinations."""
    flagged = [r for r in rows if _has_flag(r)]
    if not flagged:
        return ""
    blocks = [_flagged_detail_block(r) for r in flagged]
    return "\n\n".join(blocks)


def _build_data_quality_note(rows: list[AnalysisRow]) -> str | None:
    """Return the data quality note string, or None if all rows are complete."""
    partial_rows = [r for r in rows if r.prediction_quality == "partial"]
    if not partial_rows:
        return None
    n = len(partial_rows)
    m = len(rows)
    return (
        f"Note: {n} of {m} combinations have incomplete prediction data "
        "(missing quantiles).\n"
        "Bias and overforecast metrics for these rows may be less reliable."
    )


def _build_instructions(target_date: str) -> str:
    """Build Section 4: the report-writing instructions block."""
    return (
        f"Write the report for {target_date} following the required section structure exactly.\n"
        "Address flagged combinations in priority order: stockouts first, then "
        "overforecasting, then waste, then variability.\n"
        "Each flagged section must include Problem / Why it matters / Action.\n"
        "Calibrate language to signal strength: use direct recommendations only when "
        "metrics are well above threshold; hedge with 'consider', 'may indicate', or "
        "'if this pattern holds' when signals are weak, near-threshold, or conflicting.\n"
        "Explicitly name any data-quality or window-size limitations that affect "
        "confidence in a recommendation.\n"
        "Do not mention temperature unless |r| > 0.35.\n"
        "Do not repeat numbers from the table unless emphasising a particularly severe value."
    )


def _build_user_prompt(rows: list[AnalysisRow], target_date: str) -> str:
    """Assemble the full user prompt from all four sections."""
    parts: list[str] = []

    # Section 1 — Metrics Table
    parts.append("### Section 1 — Metrics Table\n\n" + _build_metrics_table(rows))

    # Section 2 — Flagged Combinations Detail
    flagged_detail = _build_flagged_detail_section(rows)
    if flagged_detail:
        parts.append("### Section 2 — Flagged Combinations Detail\n\n" + flagged_detail)
    else:
        parts.append(
            "### Section 2 — Flagged Combinations Detail\n\n"
            "No combinations have active flags."
        )

    # Section 3 — Data Quality Note (conditional)
    quality_note = _build_data_quality_note(rows)
    if quality_note is not None:
        parts.append("### Section 3 — Data Quality Note\n\n" + quality_note)

    # Section 4 — Instructions
    parts.append("### Section 4 — Instructions\n\n" + _build_instructions(target_date))

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
    system_prompt = _SYSTEM_PROMPT
    user_prompt = _build_user_prompt(rows, target_date)
    return system_prompt, user_prompt
