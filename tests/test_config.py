import sys
from pathlib import Path

import pytest

from coverquery import config as config_module
from coverquery.config import load_config


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


@pytest.mark.skipif(config_module.yaml is None, reason="PyYAML not installed")
def test_load_config_reads_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "coverquery.yaml"
    _write_yaml(
        config_path,
        """
        test_framework: "pytest"
        watch_paths:
          - src
        poll_interval: 1.5
        opensearch:
          host: "localhost"
          port: 9200
          index: "coverquery"
        """.strip(),
    )

    cfg = load_config(config_path, tmp_path)

    assert cfg.tests_command == "pytest"
    assert cfg.test_framework == "pytest"
    assert cfg.coverage_paths == ()
    assert cfg.watch_paths == (tmp_path / "src",)
    assert cfg.poll_interval == 1.5
    assert cfg.opensearch["host"] == "localhost"
    assert cfg.opensearch["port"] == 9200


def test_load_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml", tmp_path)


@pytest.mark.skipif(config_module.yaml is None, reason="PyYAML not installed")
def test_load_config_requires_tests_command(tmp_path: Path) -> None:
    config_path = tmp_path / "coverquery.yaml"
    _write_yaml(config_path, "watch_paths: []")

    with pytest.raises(ValueError):
        load_config(config_path, tmp_path)
