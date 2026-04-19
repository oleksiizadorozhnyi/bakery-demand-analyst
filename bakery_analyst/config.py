"""Centralised settings loaded from environment variables or a .env file.

All variable names map 1-to-1; no BAKERY_ prefix is used here because this is
the top-level package and the names are already unambiguous.
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    db_path: str = Field("bakery.db")
    seed_days: int = Field(90)
    seed_random_state: int = Field(42)

    # Seed mode
    seed_mode: Literal["synthetic", "semi_synthetic"] = Field("synthetic")
    bakery_csv_path: str = Field("data/raw/bakery_sales.csv")
    weather_cache_path: str = Field("data/raw/paris_weather.csv")

    # API server
    api_host: str = Field("127.0.0.1")
    api_port: int = Field(8000)

    # Failure simulation
    failure_enabled: bool = Field(False)
    error_500_probability: float = Field(0.0, ge=0.0, le=1.0)
    delay_probability: float = Field(0.0, ge=0.0, le=1.0)
    delay_seconds: float = Field(5.0, gt=0.0)
    partial_record_probability: float = Field(0.0, ge=0.0, le=1.0)

    # Historical windows
    main_window_days: int = Field(28, gt=0)
    recent_window_days: int = Field(14, gt=0)

    # LLM
    claude_api_key: str = Field("", alias="CLAUDE_API_KEY")
    use_mock_llm: bool = Field(False)
    claude_model: str = Field("claude-sonnet-4-6")

    model_config = {
        "env_file": ".env",
        "extra": "ignore",
        "populate_by_name": True,
    }


settings = Settings()
