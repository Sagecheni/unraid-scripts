# Repository Guidelines

## Project Structure & Module Organization
This repository is a collection of standalone Python utilities for managing media files on an Unraid server. Top-level scripts include `generate_posters.py`, `rename_short_name.py`, `split_nfo.py`, and `find_duplicate_videos.py`. `main.py` is a minimal package entry point. Project metadata lives in `pyproject.toml`, dependency locks in `uv.lock`, and working notes or specs in files such as `CLAUDE.md`, `prompt.md`, and `tree.txt`. There is no shared `src/` package or `tests/` directory yet.

## Build, Test, and Development Commands
Use `uv` for environment and script execution:

```bash
uv sync
uv run generate_posters.py --help
uv run split_nfo.py --dry-run
```

`uv sync` installs Python 3.12 dependencies from `pyproject.toml` and `uv.lock`. Run scripts through `uv run <script>` to ensure the managed environment is used. For quality checks, use Trunk:

```bash
trunk check
trunk fmt
```

`trunk check` runs configured linters; `trunk fmt` applies repo formatting.

## Coding Style & Naming Conventions
Follow existing Python style: 4-space indentation, `snake_case` for functions and variables, `UPPER_CASE` for script-level constants, and type hints where practical. Keep scripts CLI-oriented and explicit; prefer `argparse` and `pathlib.Path` for new tools. Default new file-mutating flows to a safe dry-run mode first. Preserve UTF-8-safe filename handling and avoid assumptions about local paths outside configured constants or CLI flags.

## Testing Guidelines
There is no formal automated test suite yet. Before submitting changes, run the relevant script in dry-run mode against a narrow sample path and verify logs or generated reports. If you add reusable logic, add `pytest` tests under a new `tests/` directory with names like `test_split_nfo.py`. Focus coverage on path matching, filename sanitization, and non-destructive behavior.

## Commit & Pull Request Guidelines
Recent history mostly follows Conventional Commit prefixes such as `feat:` and `fix:`. Continue using `feat:`, `fix:`, `refactor:`, or `docs:` with short imperative summaries. PRs should explain the affected workflow, note any filesystem or API prerequisites, include representative command examples, and call out whether the change was verified with `--dry-run`. Add before/after samples when renaming or reorganizing files.
