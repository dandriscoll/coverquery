"""OpenSearch indexing for CoverQuery coverage runs."""

from __future__ import annotations

import base64
import json
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib import request

from .config import Config


class IndexerError(RuntimeError):
    """Raised when OpenSearch indexing fails."""


def _find_coverage_files(run_dir: Path) -> list[Path]:
    """Find all coverage.xml files in a run directory."""
    return sorted(run_dir.rglob("coverage.xml"))


def parse_coverage_xml(coverage_path: Path) -> list[dict[str, Any]]:
    """Parse a coverage.xml file and return file-level coverage data.

    Returns a list of dicts, each containing:
        - filename: relative path to the source file
        - covered_lines: list of line numbers that were executed
    """
    tree = ET.parse(coverage_path)
    root = tree.getroot()

    files = []
    for cls in root.iter("class"):
        filename = cls.get("filename", "")
        if not filename:
            continue

        covered_lines = []
        for line in cls.iter("line"):
            hits = int(line.get("hits", "0"))
            if hits > 0:
                covered_lines.append(int(line.get("number", "0")))

        if covered_lines:
            files.append({
                "filename": filename,
                "covered_lines": sorted(covered_lines),
            })

    return files


def index_run(config: Config, run_dir: Path, commit_hash: str = "working") -> None:
    """Index a single run directory into OpenSearch.

    Args:
        config: The CoverQuery configuration.
        run_dir: Path to the run directory containing coverage.xml files.
        commit_hash: Git commit hash for this run, or 'working' for uncommitted changes.
    """
    opensearch = config.opensearch
    host = opensearch.get("host")
    port = opensearch.get("port")
    index_name = opensearch.get("index")
    if not host or not port or not index_name:
        raise IndexerError("opensearch must include host, port, and index fields.")

    scheme = opensearch.get("scheme", "http")
    username = opensearch.get("username", "")
    password = opensearch.get("password", "")

    coverage_files = _find_coverage_files(run_dir)
    if not coverage_files:
        raise IndexerError(f"No coverage.xml files found in {run_dir}.")

    _ensure_index(scheme, host, port, index_name, username, password)

    # Delete old "working" documents before re-indexing
    if commit_hash == "working":
        _delete_working_docs(scheme, host, port, index_name, username, password)

    _bulk_index(
        scheme=scheme,
        host=host,
        port=port,
        index_name=index_name,
        username=username,
        password=password,
        config=config,
        run_dir=run_dir,
        coverage_files=coverage_files,
        commit_hash=commit_hash,
    )


BATCH_SIZE = 1000

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "filename": {"type": "keyword"},
            "line": {"type": "integer"},
            "commit_hash": {"type": "keyword"},
            "run_timestamp": {"type": "keyword"},
            "test_framework": {"type": "keyword"},
            "tests": {"type": "keyword"},
        }
    },
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }
    },
}


def _delete_working_docs(
    scheme: str,
    host: str,
    port: int | str,
    index_name: str,
    username: str,
    password: str,
) -> None:
    """Delete all documents with commit_hash='working' from the index."""
    url = f"{scheme}://{host}:{port}/{index_name}/_delete_by_query"
    query = {"query": {"term": {"commit_hash": "working"}}}
    payload = json.dumps(query).encode("utf-8")
    response = _request("POST", url, username, password, payload)
    # 404 is ok if index doesn't exist yet
    if response.status not in (200, 404):
        raise IndexerError(f"Failed to delete working docs: {response.status}")


def _ensure_index(
    scheme: str,
    host: str,
    port: int | str,
    index_name: str,
    username: str,
    password: str,
) -> None:
    url = f"{scheme}://{host}:{port}/{index_name}"
    response = _request("HEAD", url, username, password)
    if response.status == 404:
        payload = json.dumps(INDEX_MAPPING).encode("utf-8")
        create_response = _request("PUT", url, username, password, payload)
        if create_response.status not in (200, 201):
            raise IndexerError(
                f"Failed to create index {index_name}: {create_response.status}"
            )
    elif response.status not in (200, 404):
        raise IndexerError(f"Unexpected index check status: {response.status}")


def _read_nodeid(test_dir: Path) -> str | None:
    """Read the nodeid from a test directory's nodeid file.

    Returns the nodeid string, or None if the file doesn't exist.
    """
    nodeid_file = test_dir / "nodeid"
    if nodeid_file.exists():
        return nodeid_file.read_text(encoding="utf-8").strip()
    return None


def _bulk_index(
    *,
    scheme: str,
    host: str,
    port: int | str,
    index_name: str,
    username: str,
    password: str,
    config: Config,
    run_dir: Path,
    coverage_files: list[Path],
    commit_hash: str,
) -> None:
    """Index coverage data with one document per (filename, line, commit_hash).

    Pass 1: Aggregate coverage from all tests into a mapping of
            (filename, line) -> set of test nodeids.
    Pass 2: Generate documents with deterministic IDs and batch index them.
    """
    # Pass 1: Aggregate coverage data
    # Key: (filename, line), Value: set of test nodeids covering that line
    coverage_map: dict[tuple[str, int], set[str]] = defaultdict(set)

    for coverage_path in coverage_files:
        # Read nodeid from file, fall back to directory name if not found
        test_dir = coverage_path.parent
        test_nodeid = _read_nodeid(test_dir) or test_dir.name
        file_coverage = parse_coverage_xml(coverage_path)

        for file_data in file_coverage:
            filename = file_data["filename"]
            for line in file_data["covered_lines"]:
                coverage_map[(filename, line)].add(test_nodeid)

    if not coverage_map:
        return

    # Pass 2: Generate documents and batch index
    url = f"{scheme}://{host}:{port}/{index_name}/_bulk"
    batch: list[str] = []

    for (filename, line), tests in coverage_map.items():
        doc_id = f"{filename}|{line}|{commit_hash}"
        doc = {
            "filename": filename,
            "line": line,
            "commit_hash": commit_hash,
            "run_timestamp": run_dir.name,
            "test_framework": config.test_framework,
            "tests": sorted(tests),
        }
        batch.append(json.dumps({"index": {"_id": doc_id}}))
        batch.append(json.dumps(doc))

        # Send batch when it reaches the limit
        if len(batch) >= BATCH_SIZE * 2:
            _send_bulk_batch(url, username, password, batch)
            batch = []

    # Send remaining documents
    if batch:
        _send_bulk_batch(url, username, password, batch)


def _send_bulk_batch(
    url: str,
    username: str,
    password: str,
    batch: list[str],
) -> None:
    """Send a batch of documents to the OpenSearch bulk API."""
    payload = "\n".join(batch) + "\n"
    response = _request("POST", url, username, password, payload.encode("utf-8"))
    if response.status not in (200, 201):
        raise IndexerError(f"Bulk index failed: {response.status}")
    body = response.read().decode("utf-8")
    data = json.loads(body)
    if data.get("errors"):
        raise IndexerError("Bulk index reported errors.")


def _request(
    method: str,
    url: str,
    username: str,
    password: str,
    data: bytes | None = None,
) -> request.addinfourl:
    headers = {"Content-Type": "application/json"}
    if username or password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "ascii"
        )
        headers["Authorization"] = f"Basic {token}"
    req = request.Request(url, method=method, headers=headers, data=data)
    try:
        return request.urlopen(req, timeout=10)
    except request.HTTPError as exc:
        return exc
