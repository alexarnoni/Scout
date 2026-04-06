import datetime
import threading
import httpx

from .sportdb import SPORTDB_BASE, HEADERS, COMPETITION_SLUG, _smart_ttl

SPORTDB_POSITION_GROUPS: dict[str, str] = {
    "GKP": "Goleiro",
    "DEF": "Defensor",
    "MID": "Meio-campo",
    "FWD": "Atacante",
}

_cache: dict = {}
_cache_lock = threading.Lock()


def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and datetime.datetime.now() < entry["expires"]:
            return entry["data"]
    return None


def _cache_set(key: str, data, ttl_seconds: int) -> None:
    with _cache_lock:
        _cache[key] = {
            "data": data,
            "expires": datetime.datetime.now() + datetime.timedelta(seconds=ttl_seconds),
        }


def get_season_results(season: str = "2026") -> list[dict]:
    key = f"season_results_{season}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    all_matches: list[dict] = []
    page = 1
    while True:
        url = f"{SPORTDB_BASE}/api/flashscore/{COMPETITION_SLUG}/{season}/results?page={page}"
        r = httpx.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_matches.extend(data)
        page += 1

    _cache_set(key, all_matches, ttl_seconds=_smart_ttl("results"))
    return all_matches


# Mapeamento positionKey (número) → grupo de posição
POSITION_KEY_MAP: dict[str, str] = {
    "1": "GKP",   # Goalkeeper
    "2": "DEF",   # Right Back / Defender
    "3": "DEF",   # Centre Back
    "4": "DEF",   # Left Back
    "5": "DEF",   # Sweeper
    "6": "MID",   # Defensive Mid
    "7": "MID",   # Central Mid
    "8": "MID",   # Attacking Mid
    "9": "MID",   # Wide Mid / Winger
    "10": "FWD",  # Second Striker
    "11": "FWD",  # Striker
}


def _merge_lineup_stats(
    lineups_data: list[dict],
    playerstats_data: list[dict],
    teams_data: list[dict],
    event_id: str,
) -> list[dict]:
    """
    Combina dados de lineup e playerstats por participantId.
    lineups_data: lista de grupos (Starting Lineups, Substitutes, Coaches)
    playerstats_data: lista de {playerId, statsKey, numericValue}
    teams_data: lista de {id, name, side} para mapear team_id → team_name
    """
    # Monta dict de stats por playerId: {playerId: {statsKey: numericValue}}
    stats_by_pid: dict[str, dict[str, float]] = {}
    for entry in playerstats_data:
        pid = str(entry.get("playerId", ""))
        if not pid:
            continue
        if pid not in stats_by_pid:
            stats_by_pid[pid] = {}
        stats_by_pid[pid][entry["statsKey"]] = entry.get("numericValue", 0)

    # Monta mapa team_id → (team_name, side)
    team_info_map: dict[str, tuple[str, str]] = {}
    for t in teams_data:
        team_info_map[str(t["id"])] = (t.get("name", ""), t.get("side", "").lower())

    # Monta mapa side → team_id
    side_to_team: dict[str, str] = {}
    for tid, (tname, side) in team_info_map.items():
        if side in ("home", "away"):
            side_to_team[side] = tid

    result: list[dict] = []

    for group in lineups_data:
        group_name = group.get("group", "")
        if group_name == "Coaches":
            continue
        is_substitute = group_name == "Substitutes"

        for side in ("home", "away"):
            team_id = side_to_team.get(side, "")
            team_name = team_info_map.get(team_id, ("", ""))[0] if team_id else ""
            players = group.get(side, [])

            for player in players:
                pid = str(player.get("participantId", ""))
                if not pid:
                    continue
                # playerType 2 = coach, skip
                if str(player.get("playerType", "1")) == "2":
                    continue

                p_stats = stats_by_pid.get(pid, {})

                # Minutos: usar matchMinutesPlayed das stats se disponível
                minutes_raw = p_stats.get("matchMinutesPlayed")
                if minutes_raw is not None and minutes_raw > 0:
                    minutes = int(minutes_raw)
                elif is_substitute:
                    minutes = 30
                else:
                    minutes = 90

                pos_key_raw = str(player.get("positionKey", ""))
                position = POSITION_KEY_MAP.get(pos_key_raw, "MID")

                record = {
                    "player_id": pid,
                    "player_name": player.get("participantName", ""),
                    "team_id": team_id,
                    "team_name": team_name,
                    "position": position,
                    "minutes": minutes,
                    "is_substitute": is_substitute,
                    "goals": int(p_stats.get("goals", 0) or 0),
                    "assists": int(p_stats.get("assistsGoal", 0) or 0),
                    "shots": int(p_stats.get("shotsTotal", 0) or 0),
                    "shots_on_target": int(p_stats.get("shotsOnTarget", 0) or 0),
                    "fouls_committed": int(p_stats.get("foulsCommitted", 0) or 0),
                    "yellow_cards": int(p_stats.get("cardsYellow", 0) or 0),
                    "red_cards": int(p_stats.get("cardsRed", 0) or 0),
                    "saves": int(p_stats.get("savesTotal", 0) or 0),
                    "goals_conceded": int(p_stats.get("goalsConceded", 0) or 0),
                    "xg": float(p_stats.get("expectedGoals", 0.0) or 0.0),
                    "rating": float(p_stats.get("fsRating", 0.0) or 0.0),
                }
                result.append(record)

    return result


def get_match_player_stats(event_id: str) -> list[dict]:
    """
    Retorna lista de jogadores com stats mergeadas para uma partida.
    Cache TTL: 86400s (24h).
    """
    key = f"match_stats_{event_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    lineups_url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/lineups"
    playerstats_url = f"{SPORTDB_BASE}/api/flashscore/match/{event_id}/playerstats"

    r_lineups = httpx.get(lineups_url, headers=HEADERS, timeout=10)
    r_lineups.raise_for_status()
    lineups_raw = r_lineups.json()

    r_stats = httpx.get(playerstats_url, headers=HEADERS, timeout=10)
    r_stats.raise_for_status()
    stats_raw = r_stats.json()

    # playerstats retorna {stats: [...], players: [...], teams: [...]}
    playerstats_data = stats_raw.get("stats", stats_raw) if isinstance(stats_raw, dict) else stats_raw
    teams_data = stats_raw.get("teams", []) if isinstance(stats_raw, dict) else []

    # lineups retorna lista de grupos com Count no final — filtra só dicts com "group"
    lineups_data = [g for g in lineups_raw if isinstance(g, dict) and "group" in g] if isinstance(lineups_raw, list) else []

    merged = _merge_lineup_stats(lineups_data, playerstats_data, teams_data, event_id)

    _cache_set(key, merged, ttl_seconds=86400)
    return merged


def _p90(value: float, minutes: float) -> float:
    if minutes <= 0:
        return 0.0
    return value / (minutes / 90)


def get_player_season_stats(
    season: str = "2026",
    min_minutes: int = 180,
) -> list[dict]:
    """
    Agrega stats de todas as partidas da temporada por jogador.
    Cache TTL: 7200s (2h).
    """
    key = f"player_season_stats_{season}_{min_minutes}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    matches = get_season_results(season)

    # Acumuladores por player_id
    players: dict[str, dict] = {}

    for match in matches:
        event_id = str(match.get("id", match.get("eventId", match.get("event_id", ""))))
        if not event_id:
            continue

        try:
            match_players = get_match_player_stats(event_id)
        except Exception:
            continue

        for p in match_players:
            pid = p["player_id"]
            if pid not in players:
                players[pid] = {
                    "player_id": pid,
                    "player_name": p["player_name"],
                    "team_id": p["team_id"],
                    "team_name": p["team_name"],
                    "position": p["position"],
                    "total_minutes": 0,
                    "matches_played": 0,
                    "goals": 0,
                    "assists": 0,
                    "shots": 0,
                    "shots_on_target": 0,
                    "fouls_committed": 0,
                    "yellow_cards": 0,
                    "red_cards": 0,
                    "saves": 0,
                    "goals_conceded": 0,
                    "xg": 0.0,
                    "_ratings": [],
                    "_clean_sheets": 0,
                }

            acc = players[pid]
            acc["total_minutes"] += p["minutes"]
            acc["matches_played"] += 1
            acc["goals"] += p["goals"]
            acc["assists"] += p["assists"]
            acc["shots"] += p["shots"]
            acc["shots_on_target"] += p["shots_on_target"]
            acc["fouls_committed"] += p["fouls_committed"]
            acc["yellow_cards"] += p["yellow_cards"]
            acc["red_cards"] += p["red_cards"]
            acc["saves"] += p["saves"]
            acc["goals_conceded"] += p.get("goals_conceded", 0)
            acc["xg"] += p.get("xg", 0.0)

            rating = p.get("rating", 0.0)
            if rating and rating > 0:
                acc["_ratings"].append(rating)

            # clean sheet: partida onde goals_conceded == 0 para este jogador
            if p.get("goals_conceded", 0) == 0:
                acc["_clean_sheets"] += 1

    result: list[dict] = []

    for pid, acc in players.items():
        position_key = acc["position"]
        position_group = SPORTDB_POSITION_GROUPS.get(position_key)
        if position_group is None:
            continue

        total_minutes = acc["total_minutes"]
        if total_minutes < min_minutes:
            continue

        matches_played = acc["matches_played"]
        ratings = acc["_ratings"]
        clean_sheets = acc["_clean_sheets"]

        goals = acc["goals"]
        shots = acc["shots"]
        saves = acc["saves"]
        goals_conceded = acc["goals_conceded"]

        avg_rating = sum(ratings) / len(ratings) if ratings else 0.0
        conversion_rate = goals / shots if shots > 0 else 0.0
        save_rate = saves / (saves + goals_conceded) if (saves + goals_conceded) > 0 else 0.0
        clean_sheet_rate = clean_sheets / matches_played if matches_played > 0 else 0.0

        result.append({
            "player_id": pid,
            "player_name": acc["player_name"],
            "team_id": acc["team_id"],
            "team_name": acc["team_name"],
            "position": position_key,
            "position_group": position_group,
            "total_minutes": total_minutes,
            "matches_played": matches_played,
            "goals": goals,
            "assists": acc["assists"],
            "shots": shots,
            "shots_on_target": acc["shots_on_target"],
            "fouls_committed": acc["fouls_committed"],
            "yellow_cards": acc["yellow_cards"],
            "red_cards": acc["red_cards"],
            "saves": saves,
            "goals_conceded": goals_conceded,
            "xg": acc["xg"],
            "goals_p90": _p90(goals, total_minutes),
            "assists_p90": _p90(acc["assists"], total_minutes),
            "shots_p90": _p90(shots, total_minutes),
            "shots_on_target_p90": _p90(acc["shots_on_target"], total_minutes),
            "fouls_p90": _p90(acc["fouls_committed"], total_minutes),
            "yellow_cards_p90": _p90(acc["yellow_cards"], total_minutes),
            "red_cards_p90": _p90(acc["red_cards"], total_minutes),
            "saves_p90": _p90(saves, total_minutes),
            "goals_conceded_p90": _p90(goals_conceded, total_minutes),
            "xg_p90": _p90(acc["xg"], total_minutes),
            "avg_rating": avg_rating,
            "conversion_rate": conversion_rate,
            "save_rate": save_rate,
            "clean_sheet_rate": clean_sheet_rate,
        })

    _cache_set(key, result, ttl_seconds=_smart_ttl("results"))
    return result
