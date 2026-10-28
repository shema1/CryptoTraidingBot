"""`.env.example` і конфіг відповідають одне одному (правило документації, T0.1)."""

from __future__ import annotations

import re

import pytest

from app.core.config import REPO_ROOT, Settings

ENV_EXAMPLE = REPO_ROOT / ".env.example"
_LINE = re.compile(r"^(?P<name>[A-Z][A-Z0-9_]*)=(?P<value>.*)$")


def _parse_env_example() -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE.match(line)
        assert match is not None, f"неочікуваний рядок у .env.example: {raw!r}"
        name = match.group("name")
        assert name not in result, f"змінна {name} у .env.example двічі"
        result[name] = match.group("value")
    return result


def _settings_env_names() -> set[str]:
    prefix = str(Settings.model_config.get("env_prefix", ""))
    names: set[str] = set()
    for field_name, field in Settings.model_fields.items():
        alias = field.validation_alias if isinstance(field.validation_alias, str) else None
        names.add((alias or prefix + field_name).upper())
    return names


def test_env_example_exists() -> None:
    assert ENV_EXAMPLE.is_file()


def test_every_config_field_is_in_env_example() -> None:
    missing = _settings_env_names() - set(_parse_env_example())
    assert missing == set(), f"додайте в .env.example: {sorted(missing)}"


def test_env_example_has_no_unknown_variables() -> None:
    unknown = set(_parse_env_example()) - _settings_env_names()
    assert unknown == set(), f"у .env.example є змінні, яких не читає конфіг: {sorted(unknown)}"


def test_env_example_has_no_values() -> None:
    with_values = {name for name, value in _parse_env_example().items() if value.strip()}
    assert with_values == set(), f".env.example не повинен містити значень: {sorted(with_values)}"


def test_empty_env_value_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "")
    assert Settings(_env_file=None).log_level == "INFO"
