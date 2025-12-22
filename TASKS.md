# CoverQuery Implementation Tasks

## Foundation

- Define the configuration format for a project (YAML or TOML) and document required keys (tests command, coverage output path, include/exclude patterns).
- Decide on the on-disk index format for coverage-to-test mappings (e.g., SQLite or JSONL) and write a small design note.
- Create a basic project layout for the CLI and core library modules.

## Core CLI

- Implement the `coverquery` entrypoint with `start` and `run` subcommands.
- Add argument parsing and help text that mirrors the behavior in `PROMPT.md`.
- Wire `coverquery run` to execute the test command and capture coverage artifacts.

## Watcher

- Implement the file watcher that monitors the project directory for changes and writes `.coverquery/.pid`.
- Ensure a clean shutdown path that removes `.pid` and leaves partial runs in a known state.
- Add safeguards against multiple watchers running concurrently.

## Coverage Collection

- Integrate with pytest to run tests and collect coverage data into a run-specific subdirectory under `.coverquery/`.
- Implement `pytest --collect-only` integration to discover tests without executing them.
- Normalize coverage data and persist results to the chosen index format.

## Query Interface

- Define the query surface for the LLM (inputs: changed files/functions; outputs: list of tests).
- Implement a first version that maps changes to tests using the coverage index.
- Add a CLI command or API endpoint for the query operation.

## Packaging

- Add build scripts for a local DEB package.
- Ensure install paths match `/usr/local/bin` and `/usr/local/lib/coverquery`.
- Document install and uninstall steps in `PROMPT.md`.

## Testing & QA

- Add unit tests for the index format, watcher lifecycle, and query logic.
- Add integration tests for `coverquery run` and `coverquery start`.
- Define a minimal coverage target and document how to run the test suite.
