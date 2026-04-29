"""Config loader.

Reads YAML files and substitutes ${ENV_VAR} placeholders from the environment.
Returns typed Pydantic settings objects so downstream code gets autocomplete +
validation rather than dict-of-dicts.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


class AppSettings(BaseSettings):
    """Top-level runtime settings sourced from environment / .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    EXECUTION_MODE: str = "mock"
    ENVIRONMENT: str = "local"
    LOG_LEVEL: str = "INFO"
    OPENLINEAGE_URL: str | None = None
    OPENLINEAGE_NAMESPACE: str = "finsight"
    SLACK_WEBHOOK_URL: str | None = None


def _substitute_env(node: Any) -> Any:
    """Recursively replace ${VAR} in any string within a parsed YAML structure."""
    if isinstance(node, dict):
        return {k: _substitute_env(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_env(v) for v in node]
    if isinstance(node, str):
        def _repl(match: re.Match[str]) -> str:
            var = match.group(1)
            value = os.getenv(var)
            if value is None:
                # leave literal so misconfig is loud rather than silently empty
                return f"${{{var}}}"
            return value
        return _ENV_VAR_PATTERN.sub(_repl, node)
    return node


@lru_cache(maxsize=8)
def load_yaml(name: str) -> dict[str, Any]:
    """Load a YAML file from config/ with env-var substitution. Cached."""
    path = CONFIG_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return _substitute_env(raw)


# ---- Typed accessors --------------------------------------------------------

class RetryConfig(BaseModel):
    attempts: int = 5
    initial_wait_seconds: float = 2.0
    backoff_factor: float = 2.0
    max_wait_seconds: float = 60.0


class SourceConfig(BaseModel):
    name: str
    kind: str
    description: str | None = None
    bronze_path: str
    incremental: dict[str, Any] = Field(default_factory=dict)
    # remaining fields kept loose to support kind-specific keys
    extra: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "SourceConfig":
        known = {"name", "kind", "description", "bronze_path", "incremental"}
        return cls(
            **{k: raw.get(k) for k in known if k in raw},
            extra={k: v for k, v in raw.items() if k not in known},
        )


def app_settings() -> AppSettings:
    return AppSettings()


def get_pipeline_config() -> dict[str, Any]:
    return load_yaml("pipeline_config.yaml")


def get_connections_config() -> dict[str, Any]:
    return load_yaml("connections.yaml")


def get_sources() -> list[SourceConfig]:
    return [SourceConfig.from_raw(s) for s in get_pipeline_config().get("sources", [])]


def get_source(name: str) -> SourceConfig:
    for s in get_sources():
        if s.name == name:
            return s
    raise KeyError(f"Source '{name}' not defined in pipeline_config.yaml")
