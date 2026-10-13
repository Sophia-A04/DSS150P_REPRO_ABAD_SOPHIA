"""Project configuration utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yml"


def load_settings(settings_path: Path | str | None = None) -> dict[str, Any]:
    """Load the project YAML configuration."""
    path = (
        Path(settings_path).expanduser().resolve()
        if settings_path is not None
        else DEFAULT_SETTINGS_PATH
    )

    if not path.exists():
        raise FileNotFoundError(f"Settings file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file)

    if not isinstance(settings, dict):
        raise ValueError(f"Settings file is empty or invalid: {path}")

    required_sections = {"project", "paths", "ingestion"}
    missing_sections = required_sections.difference(settings)

    if missing_sections:
        raise ValueError(
            "Missing required settings sections: "
            + ", ".join(sorted(missing_sections))
        )

    return settings


def resolve_project_path(path_value: str | Path) -> Path:
    """Resolve a configured path relative to the repository root."""
    path = Path(path_value).expanduser()

    if path.is_absolute():
        return path.resolve()

    return (PROJECT_ROOT / path).resolve()


def get_configured_path(settings: dict[str, Any], key: str) -> Path:
    """Return a configured path from the paths section."""
    try:
        value = settings["paths"][key]
    except KeyError as exc:
        raise KeyError(f"Missing configured path: paths.{key}") from exc

    return resolve_project_path(value)