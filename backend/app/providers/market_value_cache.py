"""Persistent disk cache for player market values.

Stores market values as JSON on disk so they survive container restarts,
reducing SportDB API calls from ~974 per restart to ~0 (with 7-day TTL).
"""

import json
import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# --- Module variables (Task 1.1) ---

CACHE_FILE_PATH: str = os.environ.get(
    "MV_CACHE_PATH", "/app/data/market_values.json"
)
TTL_SECONDS: int = 7 * 24 * 3600  # 604800 seconds = 7 days

_cache: dict[str, dict] = {}
_lock: threading.Lock = threading.Lock()


# --- Internal helpers ---

def _is_valid(entry: dict) -> bool:
    """Return True if *entry*'s ``fetched_at`` is within the TTL window."""
    try:
        fetched_at = datetime.fromisoformat(entry["fetched_at"])
        return (datetime.now() - fetched_at) < timedelta(seconds=TTL_SECONDS)
    except (KeyError, ValueError, TypeError):
        return False


def _save_to_disk() -> None:
    """Serialize ``_cache`` to JSON and write to ``CACHE_FILE_PATH``.

    Creates the parent directory if it doesn't exist.
    I/O errors are logged but never propagated.
    """
    try:
        directory = os.path.dirname(CACHE_FILE_PATH)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(CACHE_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(_cache, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.error("Failed to write cache file %s: %s", CACHE_FILE_PATH, exc)


# --- Public API ---

def init_cache() -> None:
    """Load the JSON cache file from disk into ``_cache``.

    * Missing file → empty cache (info log).
    * Invalid JSON → empty cache (warning log).
    * I/O error → empty cache (warning log).
    """
    global _cache
    with _lock:
        try:
            with open(CACHE_FILE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _cache = data
            else:
                logger.warning(
                    "Cache file %s does not contain a JSON object; starting empty",
                    CACHE_FILE_PATH,
                )
                _cache = {}
        except FileNotFoundError:
            logger.info(
                "Cache file %s not found; starting with empty cache",
                CACHE_FILE_PATH,
            )
            _cache = {}
        except json.JSONDecodeError:
            logger.warning(
                "Cache file %s contains invalid JSON; starting with empty cache",
                CACHE_FILE_PATH,
            )
            _cache = {}
        except OSError as exc:
            logger.warning(
                "I/O error reading cache file %s: %s; starting with empty cache",
                CACHE_FILE_PATH,
                exc,
            )
            _cache = {}


def get_cached_market_value(player_name: str) -> Optional[str]:
    """Return the cached market value if it exists and is within TTL, else None."""
    with _lock:
        entry = _cache.get(player_name)
        if entry is not None and _is_valid(entry):
            return entry.get("value")
        return None


def set_cached_market_value(player_name: str, value: str) -> None:
    """Store a market-value entry with the current ISO-8601 timestamp and persist."""
    with _lock:
        _cache[player_name] = {
            "value": value,
            "fetched_at": datetime.now().isoformat(),
        }
        _save_to_disk()


def get_cache_stats() -> dict:
    """Return ``{"total": int, "valid": int, "expired": int}``."""
    with _lock:
        total = len(_cache)
        valid = sum(1 for entry in _cache.values() if _is_valid(entry))
        return {"total": total, "valid": valid, "expired": total - valid}
