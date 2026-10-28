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


# --- Значення за замовчуванням і межі в документації збігаються з кодом (documentation.md, правило 4) ---

CORE_README = REPO_ROOT / "apps" / "backend" / "app" / "core" / "README.md"


def _literal_defaults() -> dict[str, str]:
    """Поля з простим default (int/bool/str) → як він має бути записаний у документації."""
    result: dict[str, str] = {}
    for name, field in Settings.model_fields.items():
        default = field.default
        if isinstance(default, bool):
            result[name] = "true" if default else "false"
        elif isinstance(default, int | str):
            result[name] = str(default)
    return result


def _env_comment(name: str) -> str:
    """Коментарі над рядком `NAME=` (до порожнього рядка), разом із коментарями сусідніх змінних групи."""
    lines = ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
    index = next(i for i, line in enumerate(lines) if line.startswith(f"{name}="))
    comments: list[str] = []
    for line in reversed(lines[:index]):
        if not line.strip():
            break
        if line.startswith("#"):
            comments.append(line)
    return "\n".join(comments)


@pytest.mark.parametrize(("field", "default"), sorted(_literal_defaults().items()))
def test_env_example_states_code_default(field: str, default: str) -> None:
    comment = _env_comment(field.upper())
    assert f"за замовчуванням {default}" in comment, f"{field.upper()}: у .env.example має бути default {default}"


@pytest.mark.parametrize(("field", "default"), sorted(_literal_defaults().items()))
def test_core_readme_states_code_default(field: str, default: str) -> None:
    row = next(
        (line for line in CORE_README.read_text(encoding="utf-8").splitlines() if line.startswith(f"| `{field}` |")),
        None,
    )
    assert row is not None, f"немає рядка для {field} у {CORE_README.name}"
    default_cell = row.split("|")[3]
    assert f"`{default}`" in default_cell, f"{field}: у README має бути default `{default}`"


def _bounds() -> dict[str, tuple[int, int]]:
    result: dict[str, tuple[int, int]] = {}
    for name, field in Settings.model_fields.items():
        ge = next((m.ge for m in field.metadata if getattr(m, "ge", None) is not None), None)
        le = next((m.le for m in field.metadata if getattr(m, "le", None) is not None), None)
        if isinstance(ge, int) and isinstance(le, int):
            result[name] = (ge, le)
    return result


def test_bounded_fields_exist() -> None:
    assert set(_bounds()) >= {"log_max_bytes", "log_backup_count"}


@pytest.mark.parametrize(("field", "bounds"), sorted(_bounds().items()))
def test_env_example_states_code_bounds(field: str, bounds: tuple[int, int]) -> None:
    ge, le = bounds
    assert f"{ge} … {le}" in _env_comment(field.upper()), f"{field.upper()}: у .env.example мають бути межі {ge} … {le}"
