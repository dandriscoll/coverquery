"""Tests for the MCP server module."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from coverquery.mcp_server import (
    _load_config,
    index_coverage_run,
    list_covered_files,
    list_indexed_tests,
    query_file_coverage,
    query_files_by_pattern,
    query_lines_for_test,
    query_tests_for_line,
    query_uncovered_lines,
    run_tests_with_coverage,
)
from coverquery.queries import CoverageResult, FileStats, QueryError, TestCoverage


@dataclass(frozen=True)
class MockConfig:
    """Mock config for testing."""

    project_root: Path
    tests_command: str
    test_framework: str
    coverage_paths: tuple[Path, ...]
    watch_paths: tuple[Path, ...]
    poll_interval: float
    opensearch: dict[str, object]


def make_mock_config(project_root: Path | None = None) -> MockConfig:
    """Create a mock config."""
    return MockConfig(
        project_root=project_root or Path("/tmp/project"),
        tests_command="pytest",
        test_framework="pytest",
        coverage_paths=(),
        watch_paths=(),
        poll_interval=2.0,
        opensearch={
            "host": "localhost",
            "port": 9200,
            "index": "test-index",
        },
    )


class TestLoadConfig:
    """Tests for _load_config."""

    def test_load_config_uses_environment_variables(self, tmp_path: Path) -> None:
        """Test that _load_config reads from environment variables."""
        config_path = tmp_path / "coverquery.yaml"
        config_path.write_text(
            """
test_framework: pytest
opensearch:
  host: localhost
  port: 9200
  index: test
""",
            encoding="utf-8",
        )

        with patch.dict(os.environ, {
            "COVERQUERY_CONFIG": str(config_path),
            "COVERQUERY_PROJECT_ROOT": str(tmp_path),
        }):
            config = _load_config()

        assert config.project_root == tmp_path
        assert config.test_framework == "pytest"

    def test_load_config_raises_on_missing_file(self, tmp_path: Path) -> None:
        """Test that _load_config raises when config file is missing."""
        with patch.dict(os.environ, {
            "COVERQUERY_CONFIG": str(tmp_path / "nonexistent.yaml"),
            "COVERQUERY_PROJECT_ROOT": str(tmp_path),
        }):
            with pytest.raises(FileNotFoundError):
                _load_config()


class TestQueryTestsForLine:
    """Tests for query_tests_for_line tool."""

    def test_returns_found_result(self) -> None:
        """Test successful query returns found=True with data."""
        mock_result = CoverageResult(
            filename="src/foo.py",
            line=42,
            commit_hash="abc123",
            tests=["test_a", "test_b"],
            run_timestamp="20241225T120000Z",
        )

        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_tests_for_line", return_value=mock_result):
                result = query_tests_for_line("src/foo.py", 42, "abc123")

        assert result["found"] is True
        assert result["filename"] == "src/foo.py"
        assert result["line"] == 42
        assert result["test_count"] == 2
        assert result["tests"] == ["test_a", "test_b"]
        assert "summary" in result

    def test_returns_not_found_when_no_coverage(self) -> None:
        """Test that missing coverage returns found=False."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_tests_for_line", return_value=None):
                result = query_tests_for_line("src/foo.py", 999)

        assert result["found"] is False
        assert "message" in result
        assert "suggestion" in result

    def test_returns_error_on_query_error(self) -> None:
        """Test that QueryError is caught and returned as error."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_tests_for_line", side_effect=QueryError("Connection failed")):
                result = query_tests_for_line("src/foo.py", 42)

        assert "error" in result
        assert "Connection failed" in result["error"]


class TestQueryLinesForTest:
    """Tests for query_lines_for_test tool."""

    def test_returns_coverage_by_file(self) -> None:
        """Test successful query returns coverage organized by file."""
        mock_result = TestCoverage(
            test_nodeid="tests/test_foo.py::test_bar",
            files={"src/a.py": [1, 2, 3], "src/b.py": [10, 20]},
            total_lines=5,
        )

        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_lines_for_test", return_value=mock_result):
                result = query_lines_for_test("tests/test_foo.py::test_bar")

        assert result["found"] is True
        assert result["total_lines_covered"] == 5
        assert result["files_covered"] == 2
        assert len(result["files"]) == 2

    def test_returns_not_found_when_no_coverage(self) -> None:
        """Test that empty coverage returns found=False."""
        mock_result = TestCoverage(
            test_nodeid="nonexistent",
            files={},
            total_lines=0,
        )

        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_lines_for_test", return_value=mock_result):
                result = query_lines_for_test("nonexistent")

        assert result["found"] is False


class TestQueryFileCoverage:
    """Tests for query_file_coverage tool."""

    def test_returns_stats_and_details(self) -> None:
        """Test successful query returns stats and line details."""
        mock_stats = FileStats(
            filename="src/foo.py",
            total_covered_lines=10,
            total_tests=3,
            commit_hash="abc123",
        )
        mock_lines = [
            CoverageResult("src/foo.py", 1, "abc123", ["t1"], ""),
            CoverageResult("src/foo.py", 2, "abc123", ["t1", "t2"], ""),
        ]

        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.get_file_stats", return_value=mock_stats):
                with patch("coverquery.mcp_server.get_tests_for_file", return_value=mock_lines):
                    result = query_file_coverage("src/foo.py")

        assert result["found"] is True
        assert result["total_covered_lines"] == 10
        assert result["unique_tests"] == 3
        assert len(result["covered_lines"]) == 2


class TestQueryUncoveredLines:
    """Tests for query_uncovered_lines tool."""

    def test_calculates_coverage_percentage(self) -> None:
        """Test that coverage percentage is calculated correctly."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.find_uncovered_lines", return_value=[2, 4, 6]):
                result = query_uncovered_lines("src/foo.py", 10)

        assert result["total_lines"] == 10
        assert result["covered_lines"] == 7
        assert result["uncovered_lines"] == 3
        assert result["coverage_percentage"] == 70.0
        assert result["uncovered_line_numbers"] == [2, 4, 6]


class TestListCoveredFiles:
    """Tests for list_covered_files tool."""

    def test_returns_file_list(self) -> None:
        """Test that file list is returned correctly."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.list_files", return_value=["a.py", "b.py"]):
                result = list_covered_files()

        assert result["file_count"] == 2
        assert result["files"] == ["a.py", "b.py"]


class TestListIndexedTests:
    """Tests for list_indexed_tests tool."""

    def test_returns_test_list(self) -> None:
        """Test that test list is returned correctly."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.list_tests", return_value=["test_a", "test_b"]):
                result = list_indexed_tests()

        assert result["test_count"] == 2
        assert result["tests"] == ["test_a", "test_b"]


class TestQueryFilesByPattern:
    """Tests for query_files_by_pattern tool."""

    def test_returns_aggregated_results(self) -> None:
        """Test that results are aggregated by file."""
        mock_results = [
            CoverageResult("src/a.py", 1, "abc", ["t1"], ""),
            CoverageResult("src/a.py", 2, "abc", ["t1", "t2"], ""),
            CoverageResult("src/b.py", 1, "abc", ["t1"], ""),
        ]

        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.query_by_pattern", return_value=mock_results):
                result = query_files_by_pattern("src/*.py")

        assert result["found"] is True
        assert result["file_count"] == 2
        assert result["total_covered_lines"] == 3

    def test_returns_not_found_when_no_matches(self) -> None:
        """Test that empty results return found=False."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server.query_by_pattern", return_value=[]):
                result = query_files_by_pattern("nonexistent/*.py")

        assert result["found"] is False


class TestRunTestsWithCoverage:
    """Tests for run_tests_with_coverage tool."""

    def test_returns_success_on_zero_exit(self) -> None:
        """Test that zero exit code returns success=True."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server._run_tests", return_value=0):
                result = run_tests_with_coverage()

        assert result["success"] is True
        assert result["return_code"] == 0
        assert "next_step" in result

    def test_returns_failure_on_nonzero_exit(self) -> None:
        """Test that nonzero exit code returns success=False."""
        with patch("coverquery.mcp_server._load_config", return_value=make_mock_config()):
            with patch("coverquery.mcp_server._run_tests", return_value=1):
                result = run_tests_with_coverage()

        assert result["success"] is False
        assert result["return_code"] == 1


class TestIndexCoverageRun:
    """Tests for index_coverage_run tool."""

    def test_indexes_latest_run_when_no_name_given(self, tmp_path: Path) -> None:
        """Test that latest run is indexed when no run_name specified."""
        config = make_mock_config(tmp_path)

        # Create fake run directories
        runs_dir = tmp_path / ".coverquery" / "runs"
        runs_dir.mkdir(parents=True)
        run1 = runs_dir / "20241224T000000Z"
        run1.mkdir()
        (run1 / "tests").mkdir()
        (run1 / "tests" / "coverage.xml").write_text("<coverage/>", encoding="utf-8")
        run2 = runs_dir / "20241225T000000Z"
        run2.mkdir()
        (run2 / "tests").mkdir()
        (run2 / "tests" / "coverage.xml").write_text("<coverage/>", encoding="utf-8")

        with patch("coverquery.mcp_server._load_config", return_value=config):
            with patch("coverquery.mcp_server.get_commit_hash", return_value="abc123"):
                with patch("coverquery.mcp_server.index_run") as mock_index:
                    result = index_coverage_run()

        assert result["success"] is True
        assert result["run_name"] == "20241225T000000Z"
        mock_index.assert_called_once()

    def test_indexes_specific_run_when_name_given(self, tmp_path: Path) -> None:
        """Test that specific run is indexed when run_name is provided."""
        config = make_mock_config(tmp_path)

        # Create fake run directory
        runs_dir = tmp_path / ".coverquery" / "runs"
        runs_dir.mkdir(parents=True)
        run = runs_dir / "20241224T000000Z"
        run.mkdir()
        (run / "tests").mkdir()
        (run / "tests" / "coverage.xml").write_text("<coverage/>", encoding="utf-8")

        with patch("coverquery.mcp_server._load_config", return_value=config):
            with patch("coverquery.mcp_server.get_commit_hash", return_value="abc123"):
                with patch("coverquery.mcp_server.index_run") as mock_index:
                    result = index_coverage_run("20241224T000000Z")

        assert result["success"] is True
        assert result["run_name"] == "20241224T000000Z"

    def test_returns_error_for_missing_run(self, tmp_path: Path) -> None:
        """Test that missing run returns error with available runs."""
        config = make_mock_config(tmp_path)

        # Create runs directory but not the requested run
        runs_dir = tmp_path / ".coverquery" / "runs"
        runs_dir.mkdir(parents=True)
        existing_run = runs_dir / "20241224T000000Z"
        existing_run.mkdir()
        (existing_run / "tests").mkdir()
        (existing_run / "tests" / "coverage.xml").write_text("<coverage/>", encoding="utf-8")

        with patch("coverquery.mcp_server._load_config", return_value=config):
            result = index_coverage_run("nonexistent")

        assert result["success"] is False
        assert "error" in result
        assert "available_runs" in result
        assert "20241224T000000Z" in result["available_runs"]
