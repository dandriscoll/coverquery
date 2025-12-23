import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from coverquery.cli import (
    _build_parser,
    _collect_files,
    _handle_init,
    _run_tests,
    _snapshot_changed,
)
from coverquery.config import Config


def test_snapshot_changed_detects_updates(tmp_path: Path) -> None:
    file_path = tmp_path / "file.txt"
    file_path.write_text("one", encoding="utf-8")

    initial = _collect_files([tmp_path])
    file_path.write_text("two", encoding="utf-8")
    current = _collect_files([tmp_path])

    assert _snapshot_changed(initial, current)


def test_run_tests_creates_run_metadata(tmp_path: Path) -> None:
    def fake_run(cmd, cwd=None, capture_output=False, text=False, env=None, shell=False):
        if "--collect-only" in cmd:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="tests/test_sample.py::test_one\n",
                stderr="",
            )
        if "coverage" in cmd and "xml" in cmd:
            output_index = cmd.index("-o") + 1
            Path(cmd[output_index]).write_text("<coverage />", encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    config = Config(
        project_root=tmp_path,
        tests_command="pytest",
        test_framework="pytest",
        coverage_paths=(),
        watch_paths=(tmp_path,),
        poll_interval=0.1,
        opensearch={},
    )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(subprocess, "run", fake_run)
    try:
        exit_code = _run_tests(config)
    finally:
        monkeypatch.undo()

    assert exit_code == 0
    runs_dir = tmp_path / ".coverquery" / "runs"
    run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
    assert len(run_dirs) == 1
    run_dir = run_dirs[0]
    assert (run_dir / "run.json").exists()
    coverage_files = list(run_dir.rglob("coverage.xml"))
    assert coverage_files


def test_init_creates_config(tmp_path: Path) -> None:
    args = type(
        "Args",
        (),
        {"config": "coverquery.yaml", "project_root": str(tmp_path)},
    )()

    exit_code = _handle_init(args)

    assert exit_code == 0
    config_path = tmp_path / "coverquery.yaml"
    assert config_path.exists()
    contents = config_path.read_text(encoding="utf-8")
    assert "test_framework" in contents
    assert "opensearch" in contents
    assert f'index: "{tmp_path.name}"' in contents


def test_init_refuses_existing_config(tmp_path: Path) -> None:
    config_path = tmp_path / "coverquery.yaml"
    config_path.write_text("tests_command: \"pytest\"", encoding="utf-8")

    args = type(
        "Args",
        (),
        {"config": "coverquery.yaml", "project_root": str(tmp_path)},
    )()

    exit_code = _handle_init(args)

    assert exit_code == 1


def test_test_subcommand_is_available() -> None:
    parser = _build_parser()
    args = parser.parse_args(["test"])

    assert args.command == "test"
    assert callable(args.func)


def test_coverquery_runs_repo_tests_and_writes_coverage(tmp_path: Path) -> None:
    if os.environ.get("COVERQUERY_SKIP_SELF_TEST") == "1":
        pytest.skip("Skipping recursive coverquery test invocation.")

    try:
        import coverage  # noqa: F401
    except ImportError:
        pytest.skip("coverage module not available")

    repo_root = Path(__file__).resolve().parents[1]
    config_path = tmp_path / "coverquery.yaml"
    config_path.write_text(
        "\n".join(
            [
                'test_framework: "pytest"',
                "watch_paths:",
                "  - .",
                "poll_interval: 2.0",
                "opensearch:",
                '  host: "localhost"',
                "  port: 9200",
                '  username: ""',
                '  password: ""',
                f'  index: "{repo_root.name}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["COVERQUERY_SKIP_SELF_TEST"] = "1"
    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "coverquery",
            "--project-root",
            str(repo_root),
            "--config",
            str(config_path),
            "test",
        ],
        cwd=repo_root,
        env=env,
    )

    assert run.returncode == 0

    runs_dir = repo_root / ".coverquery" / "runs"
    run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
    assert run_dirs
    latest_run = max(run_dirs, key=lambda path: path.stat().st_mtime)
    coverage_files = list((latest_run / "tests").rglob("coverage.xml"))
    assert coverage_files

    shutil.rmtree(repo_root / ".coverquery", ignore_errors=True)
