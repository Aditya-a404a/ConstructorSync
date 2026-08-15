"""
Centralized configuration for ConstructSync.

Uses pydantic-settings to read from environment variables and .env files.
All modules should import `settings` from here instead of using os.getenv().
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConstructSyncSettings(BaseSettings):
    """All pipeline configuration in one place."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Don't fail on extra env vars
    )

    # ── Core Pipeline ──────────────────────────────────────────────────
    constructsync_host: str = "0.0.0.0"
    constructsync_port: int = 8000
    constructsync_debug: bool = False

    # ── Constructor API ────────────────────────────────────────────────
    constructor_api_key: str = "test_api_key_12345"
    constructor_base_url: str = "http://localhost:8001"

    # ── Concurrency & Ingestion ────────────────────────────────────────
    default_batch_size: int = 1000
    initial_concurrency: int = 4
    max_concurrency: int = 32
    min_concurrency: int = 1

    # ── Sanitization Settings ──────────────────────────────────────────
    sanitize_text_fields: list[str] = ["description", "features", "about_product"]
    sanitize_id_fields: list[str] = ["sku", "item_id", "group_id", "id"]
    sanitize_numeric_fields: list[str] = ["price", "rating", "review_count"]
    sanitize_url_fields: list[str] = ["image_url", "product_url", "url"]

    # ── Storage ────────────────────────────────────────────────────────
    dlq_database_path: str = "dlq/dlq.db"
    hash_store_database_path: str = "data/hashes.db"


@lru_cache
def get_settings() -> ConstructSyncSettings:
    """Returns a cached singleton of the settings."""
    return ConstructSyncSettings()
