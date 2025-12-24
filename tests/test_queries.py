"""Tests for the OpenSearch query module."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from coverquery.queries import (
    CoverageResult,
    FileStats,
    QueryError,
    TestCoverage,
    _get_connection_params,
    _search,
    find_uncovered_lines,
    get_file_stats,
    get_lines_for_test,
    get_tests_for_file,
    get_tests_for_line,
    list_files,
    list_tests,
    query_by_pattern,
)


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


def make_config(opensearch: dict[str, Any] | None = None) -> MockConfig:
    """Create a mock config with default OpenSearch settings."""
    default_opensearch = {
        "scheme": "http",
        "host": "localhost",
        "port": 9200,
        "index": "test-index",
        "username": "user",
        "password": "pass",
    }
    if opensearch is None:
        opensearch_config = default_opensearch
    else:
        opensearch_config = dict(opensearch)
    return MockConfig(
        project_root=Path("/tmp"),
        tests_command="pytest",
        test_framework="pytest",
        coverage_paths=(),
        watch_paths=(),
        poll_interval=2.0,
        opensearch=opensearch_config,
    )


def test_get_connection_params_extracts_all_fields() -> None:
    """Test that _get_connection_params extracts all OpenSearch settings."""
    config = make_config()
    params = _get_connection_params(config)

    assert params["scheme"] == "http"
    assert params["host"] == "localhost"
    assert params["port"] == 9200
    assert params["index_name"] == "test-index"
    assert params["username"] == "user"
    assert params["password"] == "pass"


def test_get_connection_params_uses_defaults_for_missing_fields() -> None:
    """Test that missing fields use sensible defaults."""
    config = make_config({"host": "myhost", "port": 9201, "index": "myindex"})
    params = _get_connection_params(config)

    assert params["scheme"] == "http"
    assert params["host"] == "myhost"
    assert params["port"] == 9201
    assert params["index_name"] == "myindex"
    assert params["username"] == ""
    assert params["password"] == ""


def test_search_returns_hits() -> None:
    """Test that _search extracts hits from OpenSearch response."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "total": {"value": 2},
            "hits": [
                {"_source": {"filename": "a.py", "line": 1}},
                {"_source": {"filename": "b.py", "line": 2}},
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        results = _search(config, {"match_all": {}}, size=10)

    assert len(results) == 2
    assert results[0]["filename"] == "a.py"
    assert results[1]["filename"] == "b.py"


def test_search_raises_query_error_on_failure() -> None:
    """Test that _search raises QueryError on non-200 response."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 500

    with patch("coverquery.queries._request", return_value=mock_response):
        with pytest.raises(QueryError, match="Search failed with status 500"):
            _search(config, {"match_all": {}})


def test_get_tests_for_line_returns_coverage_result() -> None:
    """Test that get_tests_for_line returns a CoverageResult when found."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {
                    "_source": {
                        "filename": "src/foo.py",
                        "line": 42,
                        "commit_hash": "abc123",
                        "tests": ["test_a", "test_b"],
                        "run_timestamp": "20241225T120000Z",
                    }
                }
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_tests_for_line(config, "src/foo.py", 42, "abc123")

    assert result is not None
    assert result.filename == "src/foo.py"
    assert result.line == 42
    assert result.commit_hash == "abc123"
    assert result.tests == ["test_a", "test_b"]
    assert result.run_timestamp == "20241225T120000Z"


def test_get_tests_for_line_returns_none_when_not_found() -> None:
    """Test that get_tests_for_line returns None when no results."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {"hits": []}
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_tests_for_line(config, "src/foo.py", 999)

    assert result is None


def test_get_tests_for_line_uses_working_as_default_commit() -> None:
    """Test that get_tests_for_line defaults to 'working' commit hash."""
    config = make_config()
    captured_queries: list[dict[str, Any]] = []

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({"hits": {"hits": []}}).encode("utf-8")

    def capture_request(method: str, url: str, username: str, password: str, data: bytes | None = None) -> MagicMock:
        if data:
            captured_queries.append(json.loads(data.decode("utf-8")))
        return mock_response

    with patch("coverquery.queries._request", side_effect=capture_request):
        get_tests_for_line(config, "src/foo.py", 42)

    assert len(captured_queries) == 1
    query = captured_queries[0]["query"]["bool"]["must"]
    commit_term = next(t for t in query if "commit_hash" in t.get("term", {}))
    assert commit_term["term"]["commit_hash"] == "working"


def test_get_tests_for_file_returns_list_of_results() -> None:
    """Test that get_tests_for_file returns multiple CoverageResults."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {"_source": {"filename": "src/foo.py", "line": 1, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "src/foo.py", "line": 2, "commit_hash": "abc", "tests": ["t2"], "run_timestamp": ""}},
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        results = get_tests_for_file(config, "src/foo.py", "abc")

    assert len(results) == 2
    assert all(isinstance(r, CoverageResult) for r in results)
    assert results[0].line == 1
    assert results[1].line == 2


def test_get_lines_for_test_aggregates_by_file() -> None:
    """Test that get_lines_for_test groups lines by filename."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {"_source": {"filename": "a.py", "line": 1, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "a.py", "line": 3, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "b.py", "line": 10, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_lines_for_test(config, "t1", "abc")

    assert isinstance(result, TestCoverage)
    assert result.test_nodeid == "t1"
    assert result.total_lines == 3
    assert result.files == {"a.py": [1, 3], "b.py": [10]}


def test_get_lines_for_test_returns_empty_when_no_coverage() -> None:
    """Test that get_lines_for_test returns empty TestCoverage when no results."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({"hits": {"hits": []}}).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_lines_for_test(config, "nonexistent_test")

    assert result.total_lines == 0
    assert result.files == {}


def test_get_file_stats_returns_stats() -> None:
    """Test that get_file_stats calculates correct statistics."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {"_source": {"filename": "a.py", "line": 1, "commit_hash": "abc", "tests": ["t1", "t2"], "run_timestamp": ""}},
                {"_source": {"filename": "a.py", "line": 2, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "a.py", "line": 3, "commit_hash": "abc", "tests": ["t3"], "run_timestamp": ""}},
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_file_stats(config, "a.py", "abc")

    assert result is not None
    assert isinstance(result, FileStats)
    assert result.filename == "a.py"
    assert result.total_covered_lines == 3
    assert result.total_tests == 3  # t1, t2, t3
    assert result.commit_hash == "abc"


def test_get_file_stats_returns_none_when_no_coverage() -> None:
    """Test that get_file_stats returns None when no coverage data."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({"hits": {"hits": []}}).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        result = get_file_stats(config, "uncovered.py")

    assert result is None


def test_list_files_uses_aggregation() -> None:
    """Test that list_files uses OpenSearch aggregation."""
    config = make_config()
    captured_urls: list[str] = []

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "aggregations": {
            "unique_files": {
                "buckets": [
                    {"key": "src/a.py", "doc_count": 10},
                    {"key": "src/b.py", "doc_count": 5},
                ]
            }
        }
    }).encode("utf-8")

    def capture_request(method: str, url: str, username: str, password: str, data: bytes | None = None) -> MagicMock:
        captured_urls.append(url)
        return mock_response

    with patch("coverquery.queries._request", side_effect=capture_request):
        files = list_files(config)

    assert files == ["src/a.py", "src/b.py"]
    assert "_search" in captured_urls[0]


def test_list_tests_returns_test_nodeids() -> None:
    """Test that list_tests returns test nodeids from aggregation."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "aggregations": {
            "unique_tests": {
                "buckets": [
                    {"key": "tests/test_a.py::test_one", "doc_count": 100},
                    {"key": "tests/test_b.py::test_two", "doc_count": 50},
                ]
            }
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        tests = list_tests(config)

    assert tests == ["tests/test_a.py::test_one", "tests/test_b.py::test_two"]


def test_find_uncovered_lines_calculates_gaps() -> None:
    """Test that find_uncovered_lines identifies lines without coverage."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {"_source": {"filename": "a.py", "line": 1, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "a.py", "line": 3, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "a.py", "line": 5, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
            ]
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        uncovered = find_uncovered_lines(config, "a.py", 6, "abc")

    # Lines 2, 4, 6 are not covered
    assert uncovered == [2, 4, 6]


def test_query_by_pattern_filters_by_glob() -> None:
    """Test that query_by_pattern filters files by glob pattern."""
    config = make_config()

    # First call returns files list
    files_response = MagicMock()
    files_response.status = 200
    files_response.read.return_value = json.dumps({
        "aggregations": {
            "unique_files": {
                "buckets": [
                    {"key": "src/foo.py"},
                    {"key": "src/bar.py"},
                    {"key": "tests/test_foo.py"},
                ]
            }
        }
    }).encode("utf-8")

    # Second call returns coverage for matching files
    coverage_response = MagicMock()
    coverage_response.status = 200
    coverage_response.read.return_value = json.dumps({
        "hits": {
            "hits": [
                {"_source": {"filename": "src/foo.py", "line": 1, "commit_hash": "abc", "tests": ["t1"], "run_timestamp": ""}},
                {"_source": {"filename": "src/bar.py", "line": 1, "commit_hash": "abc", "tests": ["t2"], "run_timestamp": ""}},
            ]
        }
    }).encode("utf-8")

    responses = [files_response, coverage_response]

    with patch("coverquery.queries._request", side_effect=responses):
        results = query_by_pattern(config, "src/*.py", "abc")

    assert len(results) == 2
    filenames = {r.filename for r in results}
    assert filenames == {"src/foo.py", "src/bar.py"}


def test_query_by_pattern_returns_empty_when_no_matches() -> None:
    """Test that query_by_pattern returns empty list when no files match."""
    config = make_config()

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = json.dumps({
        "aggregations": {
            "unique_files": {
                "buckets": [
                    {"key": "src/foo.py"},
                ]
            }
        }
    }).encode("utf-8")

    with patch("coverquery.queries._request", return_value=mock_response):
        results = query_by_pattern(config, "nonexistent/*.py")

    assert results == []
