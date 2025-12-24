"""OpenSearch query operations for CoverQuery coverage data."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass
from typing import Any

from .config import Config
from .indexer import _request


class QueryError(RuntimeError):
    """Raised when an OpenSearch query fails."""


@dataclass
class CoverageResult:
    """A single coverage query result."""

    filename: str
    line: int
    commit_hash: str
    tests: list[str]
    run_timestamp: str


@dataclass
class FileStats:
    """Coverage statistics for a file."""

    filename: str
    total_covered_lines: int
    total_tests: int
    commit_hash: str


@dataclass
class TestCoverage:
    """Lines covered by a specific test."""

    __test__ = False
    test_nodeid: str
    files: dict[str, list[int]]  # filename -> list of line numbers
    total_lines: int


def _get_connection_params(config: Config) -> dict[str, Any]:
    """Extract OpenSearch connection parameters from config."""
    os_config = config.opensearch
    return {
        "scheme": os_config.get("scheme", "http"),
        "host": os_config.get("host", "localhost"),
        "port": os_config.get("port", 9200),
        "index_name": os_config.get("index", "coverquery"),
        "username": os_config.get("username", ""),
        "password": os_config.get("password", ""),
    }


def _search(
    config: Config, query: dict[str, Any], size: int = 100
) -> list[dict[str, Any]]:
    """Execute a search query against OpenSearch."""
    params = _get_connection_params(config)
    url = f"{params['scheme']}://{params['host']}:{params['port']}/{params['index_name']}/_search"

    payload = {
        "query": query,
        "size": size,
    }

    response = _request(
        "POST",
        url,
        params["username"],
        params["password"],
        json.dumps(payload).encode("utf-8"),
    )

    if response.status != 200:
        raise QueryError(f"Search failed with status {response.status}")

    body = json.loads(response.read().decode("utf-8"))
    return [hit["_source"] for hit in body.get("hits", {}).get("hits", [])]


def _aggregate(
    config: Config,
    query: dict[str, Any],
    aggs: dict[str, Any],
) -> dict[str, Any]:
    """Execute an aggregation query against OpenSearch."""
    params = _get_connection_params(config)
    url = f"{params['scheme']}://{params['host']}:{params['port']}/{params['index_name']}/_search"

    payload = {
        "size": 0,
        "query": query,
        "aggs": aggs,
    }

    response = _request(
        "POST",
        url,
        params["username"],
        params["password"],
        json.dumps(payload).encode("utf-8"),
    )

    if response.status != 200:
        raise QueryError(f"Aggregation query failed with status {response.status}")

    body = json.loads(response.read().decode("utf-8"))
    return body.get("aggregations", {})


def get_tests_for_line(
    config: Config,
    filename: str,
    line: int,
    commit_hash: str | None = None,
) -> CoverageResult | None:
    """Get all tests that cover a specific file and line.

    Args:
        config: CoverQuery configuration.
        filename: Path to the source file (as stored in coverage data).
        line: Line number.
        commit_hash: Optional commit hash filter. If None, uses 'working'.

    Returns:
        CoverageResult with tests covering this line, or None if not found.
    """
    commit = commit_hash or "working"
    query = {
        "bool": {
            "must": [
                {"term": {"filename": filename}},
                {"term": {"line": line}},
                {"term": {"commit_hash": commit}},
            ]
        }
    }

    results = _search(config, query, size=1)
    if not results:
        return None

    doc = results[0]
    return CoverageResult(
        filename=doc["filename"],
        line=doc["line"],
        commit_hash=doc["commit_hash"],
        tests=doc.get("tests", []),
        run_timestamp=doc.get("run_timestamp", ""),
    )


def get_tests_for_file(
    config: Config,
    filename: str,
    commit_hash: str | None = None,
    max_results: int = 1000,
) -> list[CoverageResult]:
    """Get all coverage data for a specific file.

    Args:
        config: CoverQuery configuration.
        filename: Path to the source file.
        commit_hash: Optional commit hash filter.
        max_results: Maximum number of line results to return.

    Returns:
        List of CoverageResult objects, one per covered line.
    """
    commit = commit_hash or "working"
    query = {
        "bool": {
            "must": [
                {"term": {"filename": filename}},
                {"term": {"commit_hash": commit}},
            ]
        }
    }

    results = _search(config, query, size=max_results)
    return [
        CoverageResult(
            filename=doc["filename"],
            line=doc["line"],
            commit_hash=doc["commit_hash"],
            tests=doc.get("tests", []),
            run_timestamp=doc.get("run_timestamp", ""),
        )
        for doc in results
    ]


def get_lines_for_test(
    config: Config,
    test_nodeid: str,
    commit_hash: str | None = None,
    max_results: int = 10000,
) -> TestCoverage:
    """Get all lines covered by a specific test.

    Args:
        config: CoverQuery configuration.
        test_nodeid: The pytest node ID (e.g., "tests/test_foo.py::test_bar").
        commit_hash: Optional commit hash filter.
        max_results: Maximum number of documents to scan.

    Returns:
        TestCoverage with all files and lines covered by this test.
    """
    commit = commit_hash or "working"
    query = {
        "bool": {
            "must": [
                {"term": {"tests": test_nodeid}},
                {"term": {"commit_hash": commit}},
            ]
        }
    }

    results = _search(config, query, size=max_results)

    files: dict[str, list[int]] = {}
    for doc in results:
        filename = doc["filename"]
        if filename not in files:
            files[filename] = []
        files[filename].append(doc["line"])

    # Sort lines within each file
    for filename in files:
        files[filename] = sorted(files[filename])

    total_lines = sum(len(lines) for lines in files.values())

    return TestCoverage(
        test_nodeid=test_nodeid,
        files=files,
        total_lines=total_lines,
    )


def get_file_stats(
    config: Config,
    filename: str,
    commit_hash: str | None = None,
) -> FileStats | None:
    """Get coverage statistics for a file.

    Args:
        config: CoverQuery configuration.
        filename: Path to the source file.
        commit_hash: Optional commit hash filter.

    Returns:
        FileStats with coverage metrics, or None if no coverage data.
    """
    results = get_tests_for_file(config, filename, commit_hash)
    if not results:
        return None

    all_tests = set()
    for result in results:
        all_tests.update(result.tests)

    return FileStats(
        filename=filename,
        total_covered_lines=len(results),
        total_tests=len(all_tests),
        commit_hash=results[0].commit_hash,
    )


def list_files(
    config: Config,
    commit_hash: str | None = None,
    max_results: int = 1000,
) -> list[str]:
    """List all files with coverage data.

    Args:
        config: CoverQuery configuration.
        commit_hash: Optional commit hash filter.
        max_results: Maximum number of unique files to return.

    Returns:
        List of filenames with coverage data.
    """
    commit = commit_hash or "working"
    query = {"term": {"commit_hash": commit}}
    aggs = {
        "unique_files": {
            "terms": {
                "field": "filename",
                "size": max_results,
            }
        }
    }

    result = _aggregate(config, query, aggs)
    buckets = result.get("unique_files", {}).get("buckets", [])
    return [bucket["key"] for bucket in buckets]


def list_tests(
    config: Config,
    commit_hash: str | None = None,
    max_results: int = 1000,
) -> list[str]:
    """List all tests with coverage data.

    Args:
        config: CoverQuery configuration.
        commit_hash: Optional commit hash filter.
        max_results: Maximum number of unique tests to return.

    Returns:
        List of test nodeids with coverage data.
    """
    commit = commit_hash or "working"
    query = {"term": {"commit_hash": commit}}
    aggs = {
        "unique_tests": {
            "terms": {
                "field": "tests",
                "size": max_results,
            }
        }
    }

    result = _aggregate(config, query, aggs)
    buckets = result.get("unique_tests", {}).get("buckets", [])
    return [bucket["key"] for bucket in buckets]


def find_uncovered_lines(
    config: Config,
    filename: str,
    total_lines: int,
    commit_hash: str | None = None,
) -> list[int]:
    """Find lines in a file that are NOT covered by any test.

    Args:
        config: CoverQuery configuration.
        filename: Path to the source file.
        total_lines: Total number of lines in the file (caller must provide).
        commit_hash: Optional commit hash filter.

    Returns:
        List of line numbers with no coverage.
    """
    covered = get_tests_for_file(config, filename, commit_hash)
    covered_lines = {result.line for result in covered}

    # Return lines 1..total_lines that aren't covered
    return [line for line in range(1, total_lines + 1) if line not in covered_lines]


def query_by_pattern(
    config: Config,
    pattern: str,
    commit_hash: str | None = None,
    max_results: int = 10000,
) -> list[CoverageResult]:
    """Query coverage data for files matching a glob pattern.

    Args:
        config: CoverQuery configuration.
        pattern: Glob pattern to match filenames (e.g., "src/**/*.py").
        commit_hash: Optional commit hash filter.
        max_results: Maximum number of results to return.

    Returns:
        List of CoverageResult objects for matching files.
    """
    # First get all unique files
    all_files = list_files(config, commit_hash, max_results=max_results)

    # Filter by glob pattern
    matching_files = [f for f in all_files if fnmatch.fnmatch(f, pattern)]

    if not matching_files:
        return []

    # Query coverage for all matching files
    commit = commit_hash or "working"
    query = {
        "bool": {
            "must": [
                {"terms": {"filename": matching_files}},
                {"term": {"commit_hash": commit}},
            ]
        }
    }

    results = _search(config, query, size=max_results)
    return [
        CoverageResult(
            filename=doc["filename"],
            line=doc["line"],
            commit_hash=doc["commit_hash"],
            tests=doc.get("tests", []),
            run_timestamp=doc.get("run_timestamp", ""),
        )
        for doc in results
    ]
