import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from coverquery.cli import get_commit_hash
from coverquery.indexer import (
    INDEX_MAPPING,
    _bulk_index,
    _delete_working_docs,
    _ensure_index,
    parse_coverage_xml,
)


def test_parse_coverage_xml(tmp_path: Path) -> None:
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text(
        """<?xml version="1.0" ?>
<coverage version="7.0" timestamp="1234567890" lines-valid="10" lines-covered="5">
    <packages>
        <package name="src" line-rate="0.5">
            <classes>
                <class name="foo.py" filename="src/foo.py" line-rate="0.5">
                    <lines>
                        <line number="1" hits="1"/>
                        <line number="2" hits="0"/>
                        <line number="3" hits="3"/>
                        <line number="4" hits="0"/>
                        <line number="5" hits="1"/>
                    </lines>
                </class>
                <class name="bar.py" filename="src/bar.py" line-rate="0.5">
                    <lines>
                        <line number="10" hits="1"/>
                        <line number="11" hits="0"/>
                        <line number="12" hits="2"/>
                    </lines>
                </class>
            </classes>
        </package>
    </packages>
</coverage>""",
        encoding="utf-8",
    )

    files = parse_coverage_xml(coverage_xml)

    assert len(files) == 2

    assert files[0]["filename"] == "src/foo.py"
    assert files[0]["covered_lines"] == [1, 3, 5]

    assert files[1]["filename"] == "src/bar.py"
    assert files[1]["covered_lines"] == [10, 12]


def test_parse_coverage_xml_skips_files_with_no_coverage(tmp_path: Path) -> None:
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text(
        """<?xml version="1.0" ?>
<coverage version="7.0">
    <packages>
        <package name="src">
            <classes>
                <class name="foo.py" filename="src/foo.py">
                    <lines>
                        <line number="1" hits="1"/>
                    </lines>
                </class>
                <class name="bar.py" filename="src/bar.py">
                    <lines>
                        <line number="1" hits="0"/>
                        <line number="2" hits="0"/>
                    </lines>
                </class>
            </classes>
        </package>
    </packages>
</coverage>""",
        encoding="utf-8",
    )

    files = parse_coverage_xml(coverage_xml)

    assert len(files) == 1
    assert files[0]["filename"] == "src/foo.py"


def test_get_commit_hash_returns_hash_for_clean_repo(tmp_path: Path) -> None:
    """Test that get_commit_hash returns the actual hash for a clean repo."""
    # Create a git repo with a commit
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (tmp_path / "file.txt").write_text("content")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    result = get_commit_hash(tmp_path)

    # Should be a 40-character hex string
    assert len(result) == 40
    assert all(c in "0123456789abcdef" for c in result)


def test_get_commit_hash_returns_working_for_dirty_repo(tmp_path: Path) -> None:
    """Test that get_commit_hash returns 'working' when there are uncommitted changes."""
    # Create a git repo with a commit
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (tmp_path / "file.txt").write_text("content")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    # Make uncommitted changes
    (tmp_path / "file.txt").write_text("modified content")

    result = get_commit_hash(tmp_path)

    assert result == "working"


def test_get_commit_hash_returns_working_for_non_git_directory(tmp_path: Path) -> None:
    """Test that get_commit_hash returns 'working' for non-git directories."""
    result = get_commit_hash(tmp_path)
    assert result == "working"


def test_index_mapping_has_expected_fields() -> None:
    """Test that INDEX_MAPPING has the expected field definitions."""
    properties = INDEX_MAPPING["mappings"]["properties"]
    assert properties["filename"]["type"] == "keyword"
    assert properties["line"]["type"] == "integer"
    assert properties["commit_hash"]["type"] == "keyword"
    assert properties["run_timestamp"]["type"] == "keyword"
    assert properties["test_framework"]["type"] == "keyword"
    assert properties["tests"]["type"] == "keyword"


def test_bulk_index_aggregates_coverage_per_line(tmp_path: Path) -> None:
    """Test that _bulk_index creates one document per (filename, line) with tests aggregated."""
    # Create run directory structure
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    # Create two test directories, each covering overlapping lines
    test1_dir = run_dir / "00001_test_a"
    test1_dir.mkdir()
    (test1_dir / "coverage.xml").write_text(
        """<?xml version="1.0" ?>
<coverage version="7.0">
    <packages>
        <package name="src">
            <classes>
                <class name="foo.py" filename="src/foo.py">
                    <lines>
                        <line number="1" hits="1"/>
                        <line number="2" hits="1"/>
                    </lines>
                </class>
            </classes>
        </package>
    </packages>
</coverage>""",
        encoding="utf-8",
    )

    test2_dir = run_dir / "00002_test_b"
    test2_dir.mkdir()
    (test2_dir / "coverage.xml").write_text(
        """<?xml version="1.0" ?>
<coverage version="7.0">
    <packages>
        <package name="src">
            <classes>
                <class name="foo.py" filename="src/foo.py">
                    <lines>
                        <line number="1" hits="1"/>
                        <line number="3" hits="1"/>
                    </lines>
                </class>
            </classes>
        </package>
    </packages>
</coverage>""",
        encoding="utf-8",
    )

    # Write nodeid files (as the CLI does when running tests)
    (test1_dir / "nodeid").write_text("tests/test_a.py::test_something", encoding="utf-8")
    (test2_dir / "nodeid").write_text("tests/test_b.py::test_other", encoding="utf-8")

    # Mock the config
    config = MagicMock()
    config.test_framework = "pytest"

    # Track bulk requests
    bulk_requests: list[str] = []

    def mock_request(method: str, url: str, username: str, password: str, data: bytes | None = None) -> MagicMock:
        response = MagicMock()
        response.status = 200
        if method == "POST" and "_bulk" in url:
            bulk_requests.append(data.decode("utf-8") if data else "")
            response.read.return_value = b'{"errors": false}'
        return response

    with patch("coverquery.indexer._request", side_effect=mock_request):
        _bulk_index(
            scheme="http",
            host="localhost",
            port=9200,
            index_name="test",
            username="",
            password="",
            config=config,
            run_dir=run_dir,
            coverage_files=list(run_dir.rglob("coverage.xml")),
            commit_hash="abc123",
        )

    # Parse the bulk request
    assert len(bulk_requests) == 1
    lines = bulk_requests[0].strip().split("\n")
    # Each document has 2 lines: metadata + document
    docs = []
    for i in range(0, len(lines), 2):
        meta = json.loads(lines[i])
        doc = json.loads(lines[i + 1])
        docs.append((meta, doc))

    # Should have 3 documents: line 1 (both tests), line 2 (test_a), line 3 (test_b)
    assert len(docs) == 3

    # Find the document for line 1 which should have both tests (using full nodeids)
    line1_doc = next(doc for _, doc in docs if doc["line"] == 1)
    assert line1_doc["filename"] == "src/foo.py"
    assert line1_doc["commit_hash"] == "abc123"
    assert sorted(line1_doc["tests"]) == [
        "tests/test_a.py::test_something",
        "tests/test_b.py::test_other",
    ]

    # Line 2 should only have test_a
    line2_doc = next(doc for _, doc in docs if doc["line"] == 2)
    assert line2_doc["tests"] == ["tests/test_a.py::test_something"]

    # Line 3 should only have test_b
    line3_doc = next(doc for _, doc in docs if doc["line"] == 3)
    assert line3_doc["tests"] == ["tests/test_b.py::test_other"]

    # Check document IDs are deterministic
    line1_meta = next(meta for meta, doc in docs if doc["line"] == 1)
    assert line1_meta["index"]["_id"] == "src/foo.py|1|abc123"
