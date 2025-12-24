"""MCP Server for CoverQuery coverage data queries."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from .cli import _run_tests, get_commit_hash
from .config import Config, load_config
from .indexer import index_run
from .queries import (
    CoverageResult,
    QueryError,
    find_uncovered_lines,
    get_file_stats,
    get_lines_for_test,
    get_tests_for_file,
    get_tests_for_line,
    list_files,
    list_tests,
    query_by_pattern,
)

# Initialize MCP server
mcp = FastMCP(
    name="coverquery",
)


def _load_config() -> Config:
    """Load CoverQuery configuration from environment or defaults."""
    config_path_str = os.environ.get("COVERQUERY_CONFIG", "coverquery.yaml")
    project_root_str = os.environ.get("COVERQUERY_PROJECT_ROOT", ".")

    project_root = Path(project_root_str).resolve()
    config_path = Path(config_path_str)
    if not config_path.is_absolute():
        config_path = project_root / config_path

    return load_config(config_path.resolve(), project_root)


def _format_coverage_results(results: list[CoverageResult]) -> list[dict[str, Any]]:
    """Format coverage results for output."""
    return [
        {
            "filename": r.filename,
            "line": r.line,
            "test_count": len(r.tests),
            "tests": r.tests,
        }
        for r in results
    ]


# =============================================================================
# HIGH PRIORITY TOOLS - Search/Query Operations
# =============================================================================


@mcp.tool()
def query_tests_for_line(
    filename: str,
    line: int,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    """Find all tests that execute a specific line in a source file.

    Use this to understand which tests would catch bugs on a particular line,
    or to identify which tests to run after modifying a line.

    Args:
        filename: Path to the source file (e.g., "src/coverquery/cli.py").
        line: The line number (1-indexed).
        commit_hash: Git commit hash to query. Defaults to "working" for
                     uncommitted changes.

    Returns:
        A dict with test information, or an error message if not found.
    """
    try:
        config = _load_config()
        result = get_tests_for_line(config, filename, line, commit_hash)

        if result is None:
            return {
                "found": False,
                "message": f"No coverage data for {filename}:{line}",
                "suggestion": "This line may not be covered by any tests, or coverage data hasn't been indexed.",
            }

        return {
            "found": True,
            "filename": result.filename,
            "line": result.line,
            "commit_hash": result.commit_hash,
            "test_count": len(result.tests),
            "tests": result.tests,
            "summary": f"Line {line} in {filename} is covered by {len(result.tests)} test(s).",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def query_lines_for_test(
    test_nodeid: str,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    """Find all source lines covered by a specific test.

    Use this to understand the scope of a test, identify what code paths
    it exercises, or find dead code.

    Args:
        test_nodeid: The pytest node ID (e.g., "tests/test_cli.py::test_init_creates_config").
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with coverage information organized by file.
    """
    try:
        config = _load_config()
        result = get_lines_for_test(config, test_nodeid, commit_hash)

        if result.total_lines == 0:
            return {
                "found": False,
                "test_nodeid": test_nodeid,
                "message": f"No coverage data found for test '{test_nodeid}'",
                "suggestion": "The test may not exist, or coverage hasn't been indexed for this test.",
            }

        # Create a summary per file
        file_summaries = [
            {"filename": fname, "line_count": len(lines), "lines": lines}
            for fname, lines in sorted(result.files.items())
        ]

        return {
            "found": True,
            "test_nodeid": result.test_nodeid,
            "total_lines_covered": result.total_lines,
            "files_covered": len(result.files),
            "files": file_summaries,
            "summary": f"Test '{test_nodeid}' covers {result.total_lines} lines across {len(result.files)} files.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def query_file_coverage(
    filename: str,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    """Get coverage statistics and details for a source file.

    Use this to understand overall test coverage for a file, identify
    which lines are tested, and see which tests cover the file.

    Args:
        filename: Path to the source file (e.g., "src/coverquery/indexer.py").
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with coverage statistics and line-by-line details.
    """
    try:
        config = _load_config()
        stats = get_file_stats(config, filename, commit_hash)

        if stats is None:
            return {
                "found": False,
                "filename": filename,
                "message": f"No coverage data found for '{filename}'",
                "suggestion": "The file may not be covered by any tests, or coverage hasn't been indexed.",
            }

        # Get line-level details
        line_results = get_tests_for_file(config, filename, commit_hash)
        line_details = [
            {"line": r.line, "test_count": len(r.tests)}
            for r in sorted(line_results, key=lambda x: x.line)
        ]

        return {
            "found": True,
            "filename": stats.filename,
            "commit_hash": stats.commit_hash,
            "total_covered_lines": stats.total_covered_lines,
            "unique_tests": stats.total_tests,
            "covered_lines": line_details,
            "summary": f"File '{filename}' has {stats.total_covered_lines} covered lines tested by {stats.total_tests} unique tests.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def query_uncovered_lines(
    filename: str,
    total_lines: int,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    """Find lines in a file that have no test coverage.

    Use this to identify code that needs more testing. Requires knowing
    the total number of lines in the file.

    Args:
        filename: Path to the source file.
        total_lines: Total number of lines in the file (you can get this by reading the file).
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with uncovered line numbers and coverage percentage.
    """
    try:
        config = _load_config()
        uncovered = find_uncovered_lines(config, filename, total_lines, commit_hash)

        covered_count = total_lines - len(uncovered)
        coverage_pct = (covered_count / total_lines * 100) if total_lines > 0 else 0

        return {
            "filename": filename,
            "total_lines": total_lines,
            "covered_lines": covered_count,
            "uncovered_lines": len(uncovered),
            "coverage_percentage": round(coverage_pct, 1),
            "uncovered_line_numbers": uncovered,
            "summary": f"File '{filename}' has {coverage_pct:.1f}% coverage ({covered_count}/{total_lines} lines). {len(uncovered)} lines need tests.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def list_covered_files(commit_hash: str | None = None) -> dict[str, Any]:
    """List all source files that have coverage data.

    Use this to get an overview of which files are being tested in the project.

    Args:
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with the list of filenames.
    """
    try:
        config = _load_config()
        files = list_files(config, commit_hash)

        return {
            "file_count": len(files),
            "files": files,
            "commit_hash": commit_hash or "working",
            "summary": f"Found {len(files)} files with coverage data.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def list_indexed_tests(commit_hash: str | None = None) -> dict[str, Any]:
    """List all tests that have been indexed with coverage data.

    Use this to see which tests exist in the coverage index.

    Args:
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with the list of test nodeids.
    """
    try:
        config = _load_config()
        tests = list_tests(config, commit_hash)

        return {
            "test_count": len(tests),
            "tests": tests,
            "commit_hash": commit_hash or "working",
            "summary": f"Found {len(tests)} tests with coverage data.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


@mcp.tool()
def query_files_by_pattern(
    pattern: str,
    commit_hash: str | None = None,
) -> dict[str, Any]:
    """Query coverage data for files matching a glob pattern.

    Use this to find coverage across multiple files matching a pattern,
    like all Python files in a directory.

    Args:
        pattern: Glob pattern to match filenames (e.g., "src/**/*.py", "tests/*.py").
        commit_hash: Git commit hash to query. Defaults to "working".

    Returns:
        A dict with coverage information for matching files.
    """
    try:
        config = _load_config()
        results = query_by_pattern(config, pattern, commit_hash)

        if not results:
            return {
                "found": False,
                "pattern": pattern,
                "message": f"No coverage data found for pattern '{pattern}'",
                "suggestion": "No files match the pattern, or coverage hasn't been indexed.",
            }

        # Group by file for summary
        files_summary: dict[str, dict[str, Any]] = {}
        for r in results:
            if r.filename not in files_summary:
                files_summary[r.filename] = {
                    "filename": r.filename,
                    "covered_lines": 0,
                    "tests": set(),
                }
            files_summary[r.filename]["covered_lines"] += 1
            files_summary[r.filename]["tests"].update(r.tests)

        file_list = [
            {
                "filename": f["filename"],
                "covered_lines": f["covered_lines"],
                "test_count": len(f["tests"]),
            }
            for f in files_summary.values()
        ]

        total_lines = sum(f["covered_lines"] for f in file_list)
        all_tests = set()
        for f in files_summary.values():
            all_tests.update(f["tests"])

        return {
            "found": True,
            "pattern": pattern,
            "commit_hash": commit_hash or "working",
            "file_count": len(file_list),
            "total_covered_lines": total_lines,
            "unique_tests": len(all_tests),
            "files": sorted(file_list, key=lambda x: x["filename"]),
            "summary": f"Pattern '{pattern}' matches {len(file_list)} files with {total_lines} covered lines.",
        }
    except QueryError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Configuration error: {e}"}


# =============================================================================
# LOW PRIORITY TOOLS - Testing/Coverage Operations
# =============================================================================


@mcp.tool()
def run_tests_with_coverage() -> dict[str, Any]:
    """Trigger a test run with coverage collection.

    This runs all tests with coverage enabled and stores the results
    for later indexing. Note: This may take a while for large test suites.

    Returns:
        A dict indicating success/failure and run details.
    """
    try:
        config = _load_config()

        return_code = _run_tests(config)

        if return_code == 0:
            return {
                "success": True,
                "return_code": return_code,
                "message": "Tests completed successfully with coverage collected.",
                "next_step": "Use 'index_coverage_run' to index the results into OpenSearch.",
            }
        else:
            return {
                "success": False,
                "return_code": return_code,
                "message": f"Tests completed with failures (exit code {return_code}).",
                "note": "Coverage data was still collected and can be indexed.",
            }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def index_coverage_run(run_name: str | None = None) -> dict[str, Any]:
    """Index coverage data from a test run into OpenSearch.

    This makes coverage data queryable through the other tools.

    Args:
        run_name: Specific run timestamp to index (e.g., "20241225T120000Z").
                  If not provided, indexes the most recent run.

    Returns:
        A dict indicating success/failure and indexing details.
    """
    try:
        config = _load_config()
        from .cli import _find_runs

        project_root = config.project_root

        if run_name:
            run_dir = project_root / ".coverquery" / "runs" / run_name
            if not run_dir.exists():
                available = [r.name for r in _find_runs(project_root)]
                return {
                    "success": False,
                    "error": f"Run directory not found: {run_name}",
                    "available_runs": available,
                }
        else:
            runs = _find_runs(project_root)
            if not runs:
                return {
                    "success": False,
                    "error": "No runs found. Run tests first with 'run_tests_with_coverage'.",
                }
            run_dir = runs[-1]  # Most recent

        commit_hash = get_commit_hash(project_root)
        index_run(config, run_dir, commit_hash)

        return {
            "success": True,
            "run_name": run_dir.name,
            "commit_hash": commit_hash,
            "message": f"Successfully indexed run '{run_dir.name}'.",
        }
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Server Entry Point
# =============================================================================


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
