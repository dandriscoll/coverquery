"""Configuration loading for CoverQuery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    import yaml
except ImportError:  # pragma: no cover - handled at runtime for missing dependency
    yaml = None

@dataclass(frozen=True)
class Config:
    project_root: Path
    tests_command: str
    test_framework: str
    coverage_paths: tuple[Path, ...]
    watch_paths: tuple[Path, ...]
    poll_interval: float
    opensearch: dict[str, object]


def _normalize_paths(base: Path, raw_paths: Iterable[str]) -> tuple[Path, ...]:
    return tuple((base / Path(item)).resolve() for item in raw_paths)


def load_config(config_path: Path, project_root: Path) -> Config:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Missing config file at {config_path}. Create it with a tests_command."
        )

    if yaml is None:
        raise RuntimeError(
            "PyYAML is required for YAML config parsing. Install with `pip install pyyaml`."
        )

    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    tests_command = data.get("tests_command")
    test_framework = data.get("test_framework", "")
    if not tests_command:
        if test_framework == "pytest":
            tests_command = "pytest"
        else:
            raise ValueError(
                "Config must define tests_command or test_framework (pytest)."
            )

    coverage_paths = _normalize_paths(project_root, data.get("coverage_paths", []))
    watch_paths = _normalize_paths(project_root, data.get("watch_paths", ["."]))
    poll_interval = float(data.get("poll_interval", 2.0))
    opensearch = data.get("opensearch", {}) or {}
    if not isinstance(opensearch, dict):
        raise ValueError("opensearch must be a mapping of connection settings.")

    return Config(
        project_root=project_root,
        tests_command=tests_command,
        test_framework=test_framework,
        coverage_paths=coverage_paths,
        watch_paths=watch_paths,
        poll_interval=poll_interval,
        opensearch=opensearch,
    )
