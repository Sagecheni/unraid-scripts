# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A collection of standalone Python scripts for managing video files on an Unraid media server (Emby). The scripts handle file renaming, deduplication, poster generation, and media library organization — primarily targeting video content stored on 115 cloud storage mounted via CloudDrive2 (CD2).

## Environment & Dependencies

- Python 3.12, managed via `uv` (see `pyproject.toml` and `uv.lock`)
- Key dependency: `openai` SDK (used to call various LLM APIs via OpenAI-compatible endpoints)
- Runtime dependency: `python-dotenv` for `.env` loading
- API keys are stored in `.env` (copy from `.env.example`)

```bash
uv sync          # install dependencies
uv run <script>  # run a script
```

## Linting

Trunk is configured for linting (`.trunk/trunk.yaml`):

```bash
trunk check      # run all linters
trunk fmt        # auto-format
```

Enabled linters: ruff, black, isort, bandit, markdownlint, prettier, taplo, trufflehog.

## Scripts

Each script is a standalone CLI tool meant to be run directly (no shared library/module structure):

- **`rename_short_name.py`** — Uses LLM API (via OpenAI SDK, globalai.vip endpoint) to shorten overly long video filenames. Also strips `www.98T.la@` markers. Configured via constants at top of file (`TARGET_DIR`, `MAX_FILENAME_LENGTH`, `DRY_RUN`).
- **`find_duplicate_videos.py`** — Finds and renames duplicate video files across directories by appending sequence numbers.
- **`generate_posters.py`** — Multi-threaded poster (thumbnail) generation using ffmpeg. Supports `argparse` CLI flags (`--search-dir`, `--dry-run`, `--force`, `--workers`, etc.). Handles graceful shutdown via SIGTERM/SIGINT.
- **`prompt.md`** — Design spec/prompt for an `organize_hanime.py` script (symlink-based media library organizer using DeepSeek API). The script itself was removed from the repo but the spec remains.

## Key Patterns

- All file-modifying scripts have a `DRY_RUN` mode (default `True`) — always respect this pattern when adding new scripts.
- Scripts operate on Unraid NAS paths (e.g., `/mnt/user/CloudNAS/...`, `/mnt/user/embydata/...`) and include rate-limiting (`time.sleep`) to avoid 115 cloud API throttling.
- `VIDEO_EXTENSIONS` sets are defined per-script for filtering video files.
- Filename sanitization must handle UTF-8 byte limits (Linux 255-byte filename limit) and illegal characters.
- LLM API calls use the OpenAI SDK with custom `base_url` endpoints (not the official OpenAI API).
