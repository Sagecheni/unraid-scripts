#!/usr/bin/env python3
# organize_hanime.py

import atexit
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from openai import OpenAI

# --- Configuration & Setup ---

# Load environment variables
load_dotenv()

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
if not DEEPSEEK_API_KEY:
    print("Error: DEEPSEEK_API_KEY not found in environment variables.")
    print("Please create a .env file based on .env.example")
    sys.exit(1)

# Default Paths (can be overridden by env vars)
SOURCE_ROOT = Path(
    os.getenv("SOURCE_ROOT", "/mnt/user/embydata/raw_links/Hanime/")
).resolve()
TARGET_ROOT = Path(
    os.getenv("TARGET_ROOT", "/mnt/user/embydata/links/HAnime/")
).resolve()
CACHE_FILE = Path("hanime_cache.json")

# Operational Config
# Operational Config
# Default to True (Safe Mode) unless explicitly set to 'false'
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
BATCH_SIZE = 30  # Number of filenames to send in one API call
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".ts", ".mov", ".wmv", ".iso", ".rmvb"}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("organize_hanime.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

if DRY_RUN:
    logger.warning("Running in DRY RUN mode. No filesystem changes will be made.")

# --- Cache System ---


class CacheManager:
    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.data: Dict[str, dict] = {}
        self.dirty = False
        self.load()
        atexit.register(self.save)

    def load(self):
        if self.cache_path.exists():
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                logger.info(f"Loaded cache with {len(self.data)} entries.")
            except Exception as e:
                logger.error(f"Failed to load cache: {e}")
                self.data = {}
        else:
            logger.info("No existing cache found. Starting fresh.")

    def save(self):
        if self.dirty:
            try:
                with open(self.cache_path, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=2)
                logger.info("Cache saved to disk.")
                self.dirty = False
            except Exception as e:
                logger.error(f"Failed to save cache: {e}")

    def get(self, key: str) -> Optional[dict]:
        return self.data.get(key)

    def set(self, key: str, value: dict):
        self.data[key] = value
        self.dirty = True


cache_manager = CacheManager(CACHE_FILE)

# --- DeepSeek API Integration ---

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

SYSTEM_PROMPT = """
You are a specialized file renaming assistant for Japanese animated series. 
Your task is to parse unstructured filenames and return structured JSON data.

Output Format: A strictly valid JSON List of objects.
Example:
[
  {
    "original_filename": "[魔人] 魔法闘姫リルスティア 第二話.mkv",
    "series_name": "魔法闘姫リルスティア",
    "standard_filename": "魔法闘姫リルスティア - S01E02.mkv"
  }
]

Rules:
1. `series_name`: Clean series name. Remove prefixes like [250131], [Group], etc.
2. `standard_filename`: Format as "{series_name} - S{season}E{episode}{extra_info}.{ext}".
   - Season defaults to 01 (S01).
   - Episode should be zero-padded (E01, E02).
   - Extra info (e.g., 4K, Uncensored) should be appended if present.
3. If specific episode info is missing, use "S01E01" or parse logically.
4. Ensure the output list length matches the input list length.
"""


def batch_parse_filenames(filenames: List[str]) -> Dict[str, dict]:
    """
    Sends a batch of filenames to DeepSeek API and returns a dict mapping filename -> parsed data.
    """
    if not filenames:
        return {}

    logger.info(f"Sending batch of {len(filenames)} files to DeepSeek API...")

    prompt_content = "Parse the following filenames:\n" + "\n".join(filenames)

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt_content},
            ],
            response_format={"type": "json_object"},  # DeepSeek supports json mode
        )

        # DeepSeek API sometimes returns markdown code block, or just raw JSON
        content = response.choices[0].message.content.strip()
        if content.startswith("```json"):
            content = content[7:-3].strip()
        elif content.startswith("```"):
            content = content[3:-3].strip()

        # Handle cases where the model might wrap the list in a key like "files": [...]
        # But our prompt asks for a direct list. Parsing carefully.
        try:
            parsed_data = json.loads(content)
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from API response.")
            logger.debug(f"Raw API response: {content}")
            return {}

        # Normalize result if it's inside a dict key
        if isinstance(parsed_data, dict):
            # Look for a list value
            for val in parsed_data.values():
                if isinstance(val, list):
                    parsed_data = val
                    break

        if not isinstance(parsed_data, list):
            logger.error("API response is not a list as expected.")
            return {}

        result_map = {}
        for item in parsed_data:
            orig = item.get("original_filename")
            if orig and orig in filenames:
                result_map[orig] = item

        logger.info(f"Successfully parsed {len(result_map)} files from batch.")
        return result_map

    except Exception as e:
        logger.error(f"API Call failed: {e}")
        return {}


# --- Core Logic ---


def get_file_hash(path: Path) -> str:
    """Returns a hash of the file path (relative to source root) to use as cache key."""
    # Using relative path as key because file content hash is too slow for large files
    # and we assume filenames don't change often without context change.
    # To be safer, we can include size.
    try:
        stat = path.stat()
        key_str = f"{path.relative_to(SOURCE_ROOT)}|{stat.st_size}"
        return hashlib.md5(key_str.encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.md5(str(path).encode("utf-8")).hexdigest()


def scan_files() -> Dict[str, Path]:
    """
    Scans SOURCE_ROOT for video files throughout all subdirectories of Layer 1 folders.
    Returns: Dict[file_hash, absolute_path]
    Key is file hash (or unique ID), Value is Path object.
    Actually, we need to preserve the Layer 1 folder name.
    """
    files_to_process = {}

    if not SOURCE_ROOT.exists():
        logger.error(f"Source root {SOURCE_ROOT} does not exist!")
        return {}

    logger.info(f"Scanning {SOURCE_ROOT}...")

    # Iterate over Layer 1 directories (e.g., 2024, 2025, Cartoon)
    for layer1_dir in SOURCE_ROOT.iterdir():
        if layer1_dir.is_dir():
            # Walk through all subdirectories of this Layer 1 directory
            for file_path in layer1_dir.rglob("*"):
                if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
                    # We store the file path.
                    # We need to associate it with its Layer 1 parent for later use.
                    files_to_process[file_path] = layer1_dir.name

    logger.info(f"Found {len(files_to_process)} video files.")
    return files_to_process


def safe_symlink(source: Path, target: Path):
    """Creates a symlink safely, handling existing files."""
    if DRY_RUN:
        logger.info(f"[DRY RUN] Link: {target} -> {source}")
        return

    if target.exists():
        if target.is_symlink() and target.readlink() == source:
            logger.debug(f"Skipping identical link: {target}")
            return

        # Conflict: Same filename but different source?
        # Handle versioning strategy: append to filename
        base_name = target.stem
        suffix = target.suffix
        counter = 1
        while target.exists():
            # If it's a symlink to the same file, break
            if target.is_symlink() and target.readlink() == source:
                return

            target = target.with_name(f"{base_name}_v{counter}{suffix}")
            counter += 1

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(source)
        logger.info(f"Linked: {target} -> {source}")
    except Exception as e:
        logger.error(f"Failed to link {source} to {target}: {e}")


def main():
    if not SOURCE_ROOT.exists():
        logger.critical(f"Source directory not found: {SOURCE_ROOT}")
        return

    valid_files = scan_files()  # Map: Path -> Layer1_Name

    batch_queue = []
    file_map_for_batch = {}  # filename -> (full_path, layer1_name, cache_key)

    # 1. Identify files needing parsing
    for file_path, layer1_name in valid_files.items():
        cache_key = get_file_hash(file_path)
        cached_result = cache_manager.get(cache_key)

        if cached_result:
            # Already have data, process immediately
            process_file_result(file_path, layer1_name, cached_result)
        else:
            # Need to ask API
            batch_queue.append(file_path.name)
            file_map_for_batch[file_path.name] = (file_path, layer1_name, cache_key)

            if len(batch_queue) >= BATCH_SIZE:
                process_batch(batch_queue, file_map_for_batch)
                batch_queue = []
                file_map_for_batch = {}

    # Process remaining batch
    if batch_queue:
        process_batch(batch_queue, file_map_for_batch)

    logger.info("Organization complete.")


def process_batch(filenames: List[str], context_map: Dict):
    results = batch_parse_filenames(filenames)

    for filename, parsed_info in results.items():
        if filename in context_map:
            file_path, layer1_name, cache_key = context_map[filename]

            # Save to cache
            cache_manager.set(cache_key, parsed_info)

            # Execute Symlink
            process_file_result(file_path, layer1_name, parsed_info)
        else:
            logger.warning(f"Received result for unknown file: {filename}")


def process_file_result(source_path: Path, layer1_name: str, info: dict):
    """
    Constructs the target path and creates the symlink.
    Target structure: TARGET_ROOT / Layer1 / SeriesName / StandardFilename
    """
    series_name = info.get("series_name", "Unknown Series").strip()
    # Sanitize series name for filesystem
    series_name = "".join([c for c in series_name if c not in '<>:"/\\|?*'])

    std_filename = info.get("standard_filename", source_path.name).strip()
    # Sanitize filename
    std_filename = "".join([c for c in std_filename if c not in '<>:"/\\|?*'])

    target_path = TARGET_ROOT / layer1_name / series_name / std_filename

    safe_symlink(source_path, target_path)


if __name__ == "__main__":
    main()
