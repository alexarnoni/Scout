"""World Cup 2026 provider — fetches data from SportDB API with thread-safe caching."""

import datetime
import os
import threading

import httpx

SPORTDB_API_KEY = os.environ.get("SPORTDB_API_KEY", "")
SPORTDB_BASE = "https://api.sportdb.dev"
COMPETITION_SLUG = "football/world:8/world-cup:lvUBR5F8"
SEASON = "2026"
HEADERS = {"X-API-Key": SPORTDB_API_KEY}
TIMEOUT = 15  # seconds

_cache: dict = {}
_cache_lock = threading.Lock()


def _smart_ttl(data_type: str) -> int:
    """TTL inteligente baseado no tipo de dado e horário BRT.

    Match window: 12h–23h BRT (UTC-3).
    """
    now = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-3)))
    hour = now.hour
    is_match_window = 12 <= hour <= 23

    if data_type == "standings":
        return 60 if is_match_window else 3600
    if data_type == "results":
        return 60 if is_match_window else 300
    if data_type == "fixtures":
        return 3600
    if data_type in ("squad", "match_stats", "lineups"):
        return 86400
    return 3600


def _cached_get(key: str, fetch_fn, data_type: str = "results"):
    """Thread-safe cache-or-fetch. Lock protects the full check-fetch-store sequence."""
    with _cache_lock:
        entry = _cache.get(key)
        now = datetime.datetime.now()
        if entry and (now - entry["ts"]).total_seconds() < _smart_ttl(data_type):
            return entry["data"]
        data = fetch_fn()
        _cache[key] = {"data": data, "ts": now}
        return data


# --- Internal fetch functions ---


def _fetch_standings() -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{SEASON}/standings"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_fixtures(page: int) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{SEASON}/fixtures?page={page}"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_results(page: int) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{SEASON}/results?page={page}"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_match_stats(event_id: str) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/stats"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_match_lineups(event_id: str) -> dict:
    url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/lineups"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_team_squad(team_slug: str, team_id: str) -> dict:
    url = f"{SPORTDB_BASE}/api/flashscore/team/{team_slug}/{team_id}/"
    r = httpx.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# --- Public functions ---


def get_copa_standings() -> list[dict]:
    return _cached_get("copa_standings", _fetch_standings, data_type="standings")


def get_copa_fixtures(page: int = 1) -> list[dict]:
    return _cached_get(
        f"copa_fixtures_{page}", lambda: _fetch_fixtures(page), data_type="fixtures"
    )


def get_copa_results(page: int = 1) -> list[dict]:
    return _cached_get(
        f"copa_results_{page}", lambda: _fetch_results(page), data_type="results"
    )


def get_match_stats(event_id: str) -> list[dict]:
    return _cached_get(
        f"copa_match_stats_{event_id}",
        lambda: _fetch_match_stats(event_id),
        data_type="match_stats",
    )


def get_match_lineups(event_id: str) -> dict:
    return _cached_get(
        f"copa_lineups_{event_id}",
        lambda: _fetch_match_lineups(event_id),
        data_type="lineups",
    )


def get_team_squad(team_slug: str, team_id: str) -> dict:
    return _cached_get(
        f"copa_squad_{team_id}",
        lambda: _fetch_team_squad(team_slug, team_id),
        data_type="squad",
    )
