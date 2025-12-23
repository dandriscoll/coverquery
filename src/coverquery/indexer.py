"""OpenSearch indexing for CoverQuery coverage runs."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any
from urllib import request

from .config import Config


class IndexerError(RuntimeError):
    """Raised when OpenSearch indexing fails."""


def index_run(config: Config, run_dir: Path) -> None:
    """Index a single run directory into OpenSearch."""
    opensearch = config.opensearch
    host = opensearch.get("host")
    port = opensearch.get("port")
    index_name = opensearch.get("index")
    if not host or not port or not index_name:
        raise IndexerError("opensearch must include host, port, and index fields.")

    scheme = opensearch.get("scheme", "http")
    username = opensearch.get("username", "")
    password = opensearch.get("password", "")

    run_metadata_path = run_dir / "run.json"
    if not run_metadata_path.exists():
        raise IndexerError(f"Missing run metadata at {run_metadata_path}.")

    run_metadata = json.loads(run_metadata_path.read_text(encoding="utf-8"))
    tests = run_metadata.get("tests", [])
    if not isinstance(tests, list):
        raise IndexerError("run.json tests must be a list.")

    _ensure_index(scheme, host, port, index_name, username, password)
    _bulk_index(
        scheme=scheme,
        host=host,
        port=port,
        index_name=index_name,
        username=username,
        password=password,
        run_metadata=run_metadata,
        tests=tests,
    )


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
        create_response = _request("PUT", url, username, password)
        if create_response.status not in (200, 201):
            raise IndexerError(
                f"Failed to create index {index_name}: {create_response.status}"
            )
    elif response.status not in (200, 404):
        raise IndexerError(f"Unexpected index check status: {response.status}")


def _bulk_index(
    *,
    scheme: str,
    host: str,
    port: int | str,
    index_name: str,
    username: str,
    password: str,
    run_metadata: dict[str, Any],
    tests: list[dict[str, Any]],
) -> None:
    url = f"{scheme}://{host}:{port}/{index_name}/_bulk"
    lines: list[str] = []
    for entry in tests:
        coverage_xml_path = Path(entry.get("coverage_xml", ""))
        coverage_xml = ""
        if coverage_xml_path.exists():
            coverage_xml = coverage_xml_path.read_text(encoding="utf-8")
        doc = {
            "run_timestamp": run_metadata.get("timestamp"),
            "test_framework": run_metadata.get("test_framework"),
            "tests_command": run_metadata.get("tests_command"),
            "nodeid": entry.get("nodeid"),
            "return_code": entry.get("return_code"),
            "coverage_xml": coverage_xml,
        }
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(doc))

    payload = "\n".join(lines) + "\n"
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
