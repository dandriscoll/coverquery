"""CoverQuery command line interface."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .config import Config, load_config
from .indexer import index_run, IndexerError

COVERQUERY_DIRNAME = ".coverquery"
RUNS_DIRNAME = "runs"
PID_FILENAME = ".pid"
DEFAULT_CONFIG_NAME = "coverquery.yaml"
DEFAULT_CONFIG_TEMPLATE = """\
# CoverQuery configuration
test_framework: "pytest"
watch_paths:
  - .
poll_interval: 2.0
opensearch:
  host: "localhost"
  port: 9200
  username: ""
  password: ""
  index: "{index_name}"
"""

EXCLUDED_DIRS = {".git", "__pycache__", COVERQUERY_DIRNAME}


def _ensure_coverquery_dir(project_root: Path) -> Path:
    cq_dir = project_root / COVERQUERY_DIRNAME
    cq_dir.mkdir(parents=True, exist_ok=True)
    (cq_dir / RUNS_DIRNAME).mkdir(parents=True, exist_ok=True)
    return cq_dir


def _pid_path(project_root: Path) -> Path:
    return project_root / COVERQUERY_DIRNAME / PID_FILENAME


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _write_pid(project_root: Path) -> None:
    pid_path = _pid_path(project_root)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")


def _remove_pid(project_root: Path) -> None:
    pid_path = _pid_path(project_root)
    if pid_path.exists():
        pid_path.unlink()


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    project_root = Path(args.project_root).resolve()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = project_root / config_path
    return project_root, config_path.resolve()


def _load_config_from_args(args: argparse.Namespace) -> Config:
    project_root, config_path = _resolve_paths(args)
    return load_config(config_path, project_root)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _file_fingerprint(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    hasher = hashlib.blake2b()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return (stat.st_mtime_ns, stat.st_size, hasher.hexdigest())


def get_commit_hash(project_root: Path) -> str:
    """Get the current git commit hash, or 'working' if there are uncommitted changes.

    Returns 'working' if:
    - Not in a git repository
    - There are staged or unstaged changes
    - There are untracked files in tracked directories
    """
    try:
        # Check if we're in a git repo and get HEAD commit
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return "working"

        commit_hash = result.stdout.strip()

        # Check for uncommitted changes (staged or unstaged)
        status_result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if status_result.returncode != 0 or status_result.stdout.strip():
            return "working"

        return commit_hash
    except FileNotFoundError:
        # git not installed
        return "working"


def _collect_files(paths: Iterable[Path]) -> dict[Path, tuple[int, int, str]]:
    snapshot: dict[Path, tuple[int, int, str]] = {}
    for base in paths:
        if base.is_file():
            snapshot[base] = _file_fingerprint(base)
            continue
        if not base.exists():
            continue
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
            for file_name in files:
                path = Path(root) / file_name
                try:
                    snapshot[path] = _file_fingerprint(path)
                except FileNotFoundError:
                    continue
    return snapshot


def _snapshot_changed(
    prev: dict[Path, tuple[int, int, str]],
    current: dict[Path, tuple[int, int, str]],
) -> bool:
    if prev.keys() != current.keys():
        return True
    for path, mtime in current.items():
        if prev.get(path) != mtime:
            return True
    return False


def _run_tests(config: Config) -> int:
    project_root = config.project_root
    cq_dir = _ensure_coverquery_dir(project_root)
    run_dir = cq_dir / RUNS_DIRNAME / _timestamp()
    run_dir.mkdir(parents=True, exist_ok=True)

    if config.test_framework != "pytest":
        raise NotImplementedError("Only pytest is supported as a test framework.")

    tests = _discover_pytest_tests(config)
    tests_dir = run_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)

    results = []
    overall_return_code = 0
    for index, nodeid in enumerate(tests, start=1):
        result = _run_pytest_with_coverage(
            config=config,
            nodeid=nodeid,
            output_root=tests_dir,
            index=index,
        )
        results.append(result)
        if result["return_code"] != 0:
            overall_return_code = result["return_code"]

    metadata = {
        "timestamp": run_dir.name,
        "test_framework": config.test_framework,
        "tests_command": config.tests_command,
        "return_code": overall_return_code,
        "tests": results,
    }
    (run_dir / "run.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    return overall_return_code


def _discover_pytest_tests(config: Config) -> list[str]:
    command = [sys.executable, "-m", "pytest", "--collect-only", "-q"]
    result = subprocess.run(
        command,
        cwd=config.project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "pytest collection failed:\n"
            f"{result.stdout}\n{result.stderr}".strip()
        )
    tests = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("WARNING"):
            continue
        if "::" in line:
            tests.append(line)
    return tests


def _run_pytest_with_coverage(
    config: Config,
    nodeid: str,
    output_root: Path,
    index: int,
) -> dict[str, object]:
    safe_name = _sanitize_nodeid(nodeid)
    test_dir = output_root / f"{index:05d}_{safe_name}"
    test_dir.mkdir(parents=True, exist_ok=True)

    # Store the original nodeid for later lookup during indexing
    (test_dir / "nodeid").write_text(nodeid, encoding="utf-8")

    coverage_file = test_dir / ".coverage"
    coverage_xml = test_dir / "coverage.xml"
    env = os.environ.copy()
    env["COVERAGE_FILE"] = str(coverage_file)

    run_command = [
        sys.executable,
        "-m",
        "coverage",
        "run",
        "-m",
        "pytest",
        nodeid,
    ]
    run_result = subprocess.run(
        run_command,
        cwd=config.project_root,
        env=env,
    )

    xml_command = [
        sys.executable,
        "-m",
        "coverage",
        "xml",
        "-o",
        str(coverage_xml),
    ]
    xml_result = subprocess.run(
        xml_command,
        cwd=config.project_root,
        env=env,
    )

    return {
        "nodeid": nodeid,
        "return_code": run_result.returncode,
        "coverage_xml": str(coverage_xml),
        "coverage_xml_return_code": xml_result.returncode,
    }


def _sanitize_nodeid(nodeid: str) -> str:
    safe_chars = []
    for char in nodeid:
        if char.isalnum() or char in {"-", "_"}:
            safe_chars.append(char)
        else:
            safe_chars.append("_")
    return "".join(safe_chars).strip("_") or "test"


def _handle_start(args: argparse.Namespace) -> int:
    config = _load_config_from_args(args)
    project_root = config.project_root
    _ensure_coverquery_dir(project_root)

    pid_path = _pid_path(project_root)
    if pid_path.exists():
        try:
            pid_value = int(pid_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pid_value = None
        if pid_value and _pid_is_running(pid_value):
            print("CoverQuery watcher already running.", file=sys.stderr)
            return 1
        _remove_pid(project_root)

    _write_pid(project_root)

    def _shutdown(signum: int, _frame: object) -> None:
        print(f"Received signal {signum}, shutting down.")
        _remove_pid(project_root)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    snapshot = _collect_files(config.watch_paths)
    print("CoverQuery watcher started.")
    while True:
        time.sleep(config.poll_interval)
        current = _collect_files(config.watch_paths)
        if _snapshot_changed(snapshot, current):
            print("Changes detected, running tests.")
            _run_tests(config)
            snapshot = current


def _handle_run(args: argparse.Namespace) -> int:
    config = _load_config_from_args(args)
    return _run_tests(config)


def _handle_test(args: argparse.Namespace) -> int:
    return _handle_run(args)


def _handle_init(args: argparse.Namespace) -> int:
    project_root, config_path = _resolve_paths(args)
    if config_path.exists():
        print(f"Config already exists at {config_path}.", file=sys.stderr)
        return 1
    config_path.parent.mkdir(parents=True, exist_ok=True)
    index_name = project_root.name or "coverquery"
    template = textwrap.dedent(DEFAULT_CONFIG_TEMPLATE).format(index_name=index_name)
    config_path.write_text(template, encoding="utf-8")
    print(f"Wrote config to {config_path}.")
    return 0


def _find_runs(project_root: Path) -> list[Path]:
    """Return all run directories sorted by name (oldest first)."""
    runs_dir = project_root / COVERQUERY_DIRNAME / RUNS_DIRNAME
    if not runs_dir.exists():
        return []
    return sorted(
        [d for d in runs_dir.iterdir() if d.is_dir() and list(d.rglob("coverage.xml"))]
    )


def _handle_index(args: argparse.Namespace) -> int:
    config = _load_config_from_args(args)
    project_root = config.project_root

    if args.run:
        run_dir = project_root / COVERQUERY_DIRNAME / RUNS_DIRNAME / args.run
        if not run_dir.exists():
            print(f"Run directory not found: {run_dir}", file=sys.stderr)
            return 1
        runs_to_index = [run_dir]
    elif args.all:
        runs_to_index = _find_runs(project_root)
        if not runs_to_index:
            print("No runs found to index.", file=sys.stderr)
            return 1
    else:
        runs = _find_runs(project_root)
        if not runs:
            print("No runs found. Run tests first with 'coverquery run'.", file=sys.stderr)
            return 1
        runs_to_index = [runs[-1]]

    commit_hash = get_commit_hash(project_root)
    indexed_count = 0
    for run_dir in runs_to_index:
        try:
            index_run(config, run_dir, commit_hash)
            print(f"Indexed {run_dir.name} (commit: {commit_hash[:8] if commit_hash != 'working' else 'working'})")
            indexed_count += 1
        except IndexerError as exc:
            print(f"Failed to index {run_dir.name}: {exc}", file=sys.stderr)
            if not args.all:
                return 1

    print(f"Indexed {indexed_count} run(s).")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="coverquery")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_NAME,
        help="Path to the CoverQuery YAML config file.",
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Project root to monitor and run tests from.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Start the file watcher.")
    start.set_defaults(func=_handle_start)

    run = subparsers.add_parser("run", help="Run tests and capture coverage.")
    run.set_defaults(func=_handle_run)

    test = subparsers.add_parser(
        "test",
        help="Discover tests then run each with coverage enabled.",
    )
    test.set_defaults(func=_handle_test)

    init = subparsers.add_parser("init", help="Write a default config file.")
    init.set_defaults(func=_handle_init)

    index = subparsers.add_parser(
        "index",
        help="Index coverage data from runs into OpenSearch.",
    )
    index.add_argument(
        "--run",
        metavar="RUN_NAME",
        help="Index a specific run by its timestamp name (e.g., 20241225T120000Z).",
    )
    index.add_argument(
        "--all",
        action="store_true",
        help="Index all available runs.",
    )
    index.set_defaults(func=_handle_index)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
