import datetime
import threading
import httpx
from typing import Optional

import os
SPORTDB_API_KEY = os.environ.get("SPORTDB_API_KEY", "")
SPORTDB_BASE = "https://api.sportdb.dev"
COMPETITION_SLUG = "football/brazil:39/serie-a-betano:Yq4hUnzQ"

HEADERS = {"X-API-Key": SPORTDB_API_KEY}

_cache: dict = {}
_cache_lock = threading.Lock()


def _smart_ttl(data_type: str) -> int:
    """TTL inteligente baseado no tipo de dado e horário BRT."""
    now = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-3)))
    hour = now.hour
    is_match_window = 14 <= hour <= 23

    if data_type == "standings":
        return 300 if is_match_window else 1800
    if data_type == "results":
        return 300 if is_match_window else 3600
    if data_type == "fixtures":
        return 7200
    if data_type == "match_stats":
        return 86400
    if data_type == "averages":
        return 600 if is_match_window else 3600
    if data_type == "player":
        return 86400
    return 1800


def _cached_get(key: str, fetch_fn, data_type: str = "results"):
    with _cache_lock:
        entry = _cache.get(key)
        now = datetime.datetime.now()
        if entry and (now - entry['ts']).total_seconds() < _smart_ttl(data_type):
            return entry['data']
        data = fetch_fn()
        _cache[key] = {'data': data, 'ts': now}
        return data


def get_standings(season: str = "2026") -> list[dict]:
    return _cached_get(f'standings_{season}', lambda: _fetch_standings(season), data_type="standings")


def _fetch_standings(season: str = "2026") -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{season}/standings"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_season_results(season: str = "2026", page: int = 1) -> list[dict]:
    return _cached_get(f'results_{season}_{page}', lambda: _fetch_results(season, page), data_type="results")


def _fetch_results(season: str = "2026", page: int = 1) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{season}/results?page={page}"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_season_fixtures(season: str = "2026", page: int = 1) -> list[dict]:
    return _cached_get(f'fixtures_{season}_{page}', lambda: _fetch_fixtures(season, page), data_type="fixtures")


def _fetch_fixtures(season: str = "2026", page: int = 1) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{season}/fixtures?page={page}"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_match_lineup(event_id: str) -> dict:
    url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/lineups"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    starters = {}
    subs = {}
    for group in data:
        if group.get("group") == "Starting Lineups":
            starters = group
        elif group.get("group") == "Substitutes":
            subs = group
    return {"starters": starters, "subs": subs}


TEAM_SLUG_MAP = {
    "Athletico-PR": "athletico-pr",
    "Atlético-MG": "atletico-mg",
    "Bahia": "bahia",
    "Botafogo": "botafogo-rj",
    "Chapecoense": "chapecoense-sc",
    "Corinthians": "corinthians",
    "Coritiba": "coritiba",
    "Cruzeiro": "cruzeiro",
    "Flamengo": "flamengo-rj",
    "Fluminense": "fluminense",
    "Grêmio": "gremio",
    "Internacional": "internacional",
    "Mirassol": "mirassol",
    "Palmeiras": "palmeiras",
    "Red Bull Bragantino": "bragantino",
    "Remo": "remo",
    "Santos": "santos",
    "São Paulo": "sao-paulo",
    "Vasco da Gama": "vasco",
    "Vitória": "vitoria",
}


def get_team_slug(team_name: str) -> Optional[str]:
    return TEAM_SLUG_MAP.get(team_name)


def _fetch_match_stats(event_id: str) -> list[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/stats"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_match_stats(event_id: str) -> list[dict]:
    return _cached_get(f'stats_{event_id}', lambda: _fetch_match_stats(event_id), data_type="match_stats")


def _fetch_match_details(event_id: str) -> dict:
    url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/details?with_events=true"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_match_details(event_id: str) -> dict:
    """GET /api/flashscore/match/{eventId}/details?with_events=true"""
    return _cached_get(f'details_{event_id}', lambda: _fetch_match_details(event_id), data_type="match_stats")


def get_team_season_averages(team_id: str, season: str = "2026") -> dict:
    return _cached_get(f'averages_{team_id}_{season}', lambda: _fetch_team_averages(team_id, season), data_type="averages")


def _fetch_team_averages(team_id: str, season: str = "2026") -> dict:
    results = _fetch_results(season)
    event_ids = []
    for match in reversed(results):
        home = match.get("homeParticipantIds", "")
        away = match.get("awayParticipantIds", "")
        if team_id in (home, away):
            event_ids.append((match.get("eventId"), home == team_id))
        if len(event_ids) >= 5:
            break

    if not event_ids:
        return {}

    STAT_MAP = {
        "Ball possession": "possession",
        "Total shots": "shots",
        "Shots on target": "shots_on_target",
        "Expected goals (xG)": "xg",
        "Corner kicks": "corners",
        "Passes": "passes",
        "Fouls": "fouls",
    }
    totals: dict[str, list] = {k: [] for k in STAT_MAP.values()}

    for event_id, is_home in event_ids:
        try:
            raw = _fetch_match_stats(event_id)
            period = next((p for p in raw if p["period"] == "Match"), None)
            if not period:
                continue
            for s in period["stats"]:
                key = STAT_MAP.get(s["statName"])
                if key:
                    val_str = s["homeValue"] if is_home else s["awayValue"]
                    try:
                        num = float(str(val_str).replace('%', '').split(' ')[0])
                        totals[key].append(num)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            continue

    return {k: round(sum(v) / len(v), 2) if v else 0 for k, v in totals.items()}


def get_last_match_event_id(team_name_url: str, season: str = "2026") -> Optional[str]:
    """Busca o eventId da última partida finalizada de um time."""
    results = get_season_results(season)
    for match in results:
        home = match.get("homeParticipantNameUrl", "")
        away = match.get("awayParticipantNameUrl", "")
        if team_name_url in (home, away):
            return match.get("eventId")
    return None


def get_player_profile(player_slug: str, player_id: str) -> dict:
    return _cached_get(f'player_{player_id}', lambda: _fetch_player_profile(player_slug, player_id), data_type="player")


def _fetch_player_profile(player_slug: str, player_id: str) -> dict:
    url = f"{SPORTDB_BASE}/api/flashscore/player/{player_slug}/{player_id}"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def search_player(name: str) -> Optional[dict]:
    return _cached_get(f'search_{name}', lambda: _fetch_search_player(name))


def _fetch_search_player(name: str) -> Optional[dict]:
    url = f"{SPORTDB_BASE}/api/flashscore/search?q={name}&type=player"
    r = httpx.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None


def get_player_market_value(name: str) -> Optional[str]:
    try:
        result = search_player(name)
        if not result:
            return None
        parts = result["link"].replace("/api/flashscore/player/", "").strip("/").split("/")
        if len(parts) < 2:
            return None
        profile = get_player_profile(parts[0], parts[1])
        return profile.get("marketValue")
    except Exception:
        return None
