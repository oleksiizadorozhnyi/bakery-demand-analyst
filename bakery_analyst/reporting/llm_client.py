"""LLM client for generating bakery demand reports via Claude API or mock."""

from __future__ import annotations

from bakery_analyst.config import settings
from bakery_analyst.models.domain_models import AnalysisRow


def generate_report(
    system_prompt: str,
    user_prompt: str,
    target_date: str,
    n_rows: int,
    rows: list[AnalysisRow],
) -> str:
    """Generate report text via Claude API or mock.

    Args:
        system_prompt: The system prompt defining the analyst role and format.
        user_prompt: The user prompt containing compact data and instructions.
        target_date: The date string for which the report is being generated.
        n_rows: Total number of shop/product combinations analysed.
        rows: List of AnalysisRow objects used to summarise risk flags in mock mode.

    Returns:
        The report as a string.
    """
    if settings.use_mock_llm:
        return _generate_mock_report(target_date, n_rows, rows)
    return _generate_real_report(system_prompt, user_prompt)


def _generate_real_report(system_prompt: str, user_prompt: str) -> str:
    """Call the Claude API with prompt caching on the system prompt."""
    import anthropic

    client = anthropic.Anthropic(api_key=settings.claude_api_key)

    response = client.messages.create(
        model=settings.claude_model,
        max_tokens=1800,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )
    report_text: str = response.content[0].text
    return report_text


def _generate_mock_report(
    target_date: str,
    n_rows: int,
    rows: list[AnalysisRow],
) -> str:
    """Return a clearly labelled placeholder report."""
    flag_parts: list[str] = []
    for row in rows:
        codes: list[str] = []
        if row.high_waste_flag:
            codes.append("high_waste")
        if row.frequent_stockout_flag:
            codes.append("frequent_stockout")
        if row.high_variability_flag:
            codes.append("high_variability")
        if row.persistent_overforecast_flag:
            codes.append("persistent_overforecast")
        if row.incomplete_prediction_flag:
            codes.append("incomplete_prediction")
        if codes:
            flag_parts.append(f"{row.shop_id}/{row.product_code}: {', '.join(codes)}")

    if flag_parts:
        flags_section = "\n".join(f"  - {f}" for f in flag_parts)
    else:
        flags_section = "  - None detected"

    return (
        f"[MOCK REPORT — USE_MOCK_LLM=true]\n"
        f"\n"
        f"## Bakery Demand Report — {target_date}\n"
        f"\n"
        f"This is a mock report. Set USE_MOCK_LLM=false and provide CLAUDE_API_KEY to generate a real report.\n"
        f"\n"
        f"## Situation Summary\n"
        f"- {n_rows} shop/product combinations analysed.\n"
        f"- Risk flags detected:\n"
        f"{flags_section}\n"
        f"\n"
        f"## Root Cause Assessment\n"
        f"- [Root cause analysis would appear here in a real report.]\n"
        f"\n"
        f"## Recommended Actions\n"
        f"- [Concrete operational recommendations would appear here.]\n"
        f"\n"
        f"## What to Monitor Next\n"
        f"- [Follow-up monitoring guidance would appear here.]\n"
        f"\n"
        f"[End of mock report]"
    )
