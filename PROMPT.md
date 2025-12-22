# CoverQuery

CoverQuery manages code coverage data for multiple projects and lets an LLM query which tests to run and which parts of the code are covered.

# Repository Guidelines

## Project Structure & Module Organization

- Each project contains a `.coverquery/` directory.
- `.coverquery/` holds per-run subdirectories with temporary coverage results and a `.pid` file for the watcher process.
- When implementation lands, keep CLI code and packaging metadata visible from the root and document any new top-level directories here.

## Build, Test, and Development Commands

- `coverquery start`: starts the file watcher, reads project config, and writes `.coverquery/.pid`.
- `coverquery run`: triggers tests, collects coverage data, and writes a run-specific folder under `.coverquery/`.
- `coverquery test`: discovers pytest tests and runs each test with coverage enabled, storing per-test reports under `.coverquery/`.
- `pytest --collect-only`: used internally to list tests without executing them.
- DEB packaging (planned): `sudo apt install ./coverquery.deb` installs to `/usr/local/bin` and `/usr/local/lib/coverquery`.

## Coding Style & Naming Conventions

- No formatter or linter is enforced yet. Use consistent indentation (4 spaces for Python) and descriptive names.
- CLI commands should be lower-case and verb-based, such as `coverquery start` and `coverquery run`.

## Testing Guidelines

- Test discovery relies on `pytest --collect-only` and pytest naming (`test_*.py`).
- Add coverage or test runner requirements here once defined.

## Commit & Pull Request Guidelines

- Git history uses short, imperative subjects (example: "Add prompt"). Keep commits concise.
- PRs should include a brief summary, testing notes (or "not run" with a reason), and links to relevant issues or docs updates.

## Security & Configuration Tips

- Treat `.coverquery/` as generated data; do not commit run artifacts or `.pid` files.
- If adding configuration files, document required keys and defaults in this file.
