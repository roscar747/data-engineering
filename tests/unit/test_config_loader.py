from __future__ import annotations

import os

from src.utils.config_loader import _substitute_env, app_settings


def test_substitute_env_replaces_placeholders(monkeypatch) -> None:
    monkeypatch.setenv("FOO", "bar")
    out = _substitute_env({"key": "prefix-${FOO}-suffix", "list": ["${FOO}", "static"]})
    assert out["key"] == "prefix-bar-suffix"
    assert out["list"] == ["bar", "static"]


def test_substitute_env_leaves_unknown_var_literal(monkeypatch) -> None:
    monkeypatch.delenv("NOT_SET", raising=False)
    out = _substitute_env("${NOT_SET}")
    assert out == "${NOT_SET}"


def test_app_settings_defaults() -> None:
    os.environ.pop("EXECUTION_MODE", None)
    s = app_settings()
    assert s.EXECUTION_MODE in {"mock", "live"}
