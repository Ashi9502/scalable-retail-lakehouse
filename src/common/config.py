from __future__ import annotations

from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config() -> dict:
    path = PROJECT_ROOT / "config" / "project.yml"
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def project_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path
