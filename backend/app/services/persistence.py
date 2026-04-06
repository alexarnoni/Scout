"""
Camada de persistência compartilhada entre scripts de sincronização.

Todas as funções de upsert e parsing de stats vivem aqui para evitar
duplicação entre sync_date.py, backfill.py e outros scripts futuros.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from app.models import Match, Player, PlayerMatchStats, Team, TeamMatchStats


def parse_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "T" not in value and " " not in value:
        value = f"{value}T00:00:00"
    return datetime.fromisoformat(value)


def extract_xg(stats_json) -> float | None:
    def to_float(value) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip().lower().replace("%", "").replace(",", ".")
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    direct_keys = [
        "xg",
        "xG",
        "xGoals",
        "expectedGoals",
        "expected_goals",
        "expectedGoalsFor",
    ]

    def scan(obj) -> float | None:
        if isinstance(obj, dict):
            for key in direct_keys:
                if key in obj:
                    direct_val = to_float(obj.get(key))
                    if direct_val is not None:
                        return direct_val

            name = obj.get("name") or obj.get("label") or obj.get("type") or obj.get("title")
            if isinstance(name, str):
                label = name.lower()
                if "xg" in label or "expected goals" in label or "expected goal" in label:
                    val = (
                        obj.get("value")
                        or obj.get("stat")
                        or obj.get("valueText")
                        or obj.get("valueStr")
                    )
                    named_val = to_float(val)
                    if named_val is not None:
                        return named_val

            for value in obj.values():
                found = scan(value)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = scan(item)
                if found is not None:
                    return found
        return None

    return scan(stats_json)


def upsert_match(db, competition_id: int, source: str, payload: dict) -> tuple[Match, bool]:
    external_id = payload.get("external_id")
    match = None
    if external_id:
        match = db.execute(
            select(Match).where(
                Match.external_source == source,
                Match.external_id == external_id,
            )
        ).scalar_one_or_none()

    match_date_time = parse_datetime(payload["date"])
    home_team = db.execute(
        select(Team).where(
            Team.competition_id == competition_id,
            Team.name == payload["home_team"],
        )
    ).scalar_one_or_none()
    away_team = db.execute(
        select(Team).where(
            Team.competition_id == competition_id,
            Team.name == payload["away_team"],
        )
    ).scalar_one_or_none()

    if not home_team or not away_team:
        raise ValueError("Home or away team not found for match payload")

    if not match:
        match = db.execute(
            select(Match).where(
                Match.competition_id == competition_id,
                Match.match_date_time == match_date_time,
                Match.home_team_id == home_team.id,
                Match.away_team_id == away_team.id,
            )
        ).scalar_one_or_none()

    created = False
    if not match:
        match = Match(
            competition_id=competition_id,
            round_number=payload["round"],
            match_date_time=match_date_time,
            home_team_id=home_team.id,
            away_team_id=away_team.id,
            status=payload.get("status", "scheduled"),
            score_home=payload.get("score_home"),
            score_away=payload.get("score_away"),
            external_source=source,
            external_id=external_id,
            external_ids={source: external_id} if external_id else None,
            sportdb_event_id=payload.get("sportdb_event_id"),
        )
        db.add(match)
        db.flush()
        created = True
    else:
        match.round_number = payload["round"]
        match.status = payload.get("status", match.status)
        match.score_home = payload.get("score_home")
        match.score_away = payload.get("score_away")
        match.external_source = source
        match.external_id = external_id
        if external_id:
            ids = match.external_ids or {}
            ids[source] = external_id
            match.external_ids = ids
        match.sportdb_event_id = payload.get("sportdb_event_id")

    return match, created


def upsert_team_stats(db, match: Match, payload: dict) -> bool:
    team = db.execute(
        select(Team).where(
            Team.id == payload["team_id"],
        )
    ).scalar_one_or_none()
    if not team:
        raise ValueError("Team not found for stats payload")

    stats = db.execute(
        select(TeamMatchStats).where(
            TeamMatchStats.match_id == match.id,
            TeamMatchStats.team_id == team.id,
        )
    ).scalar_one_or_none()

    fields = {
        "match_id": match.id,
        "team_id": team.id,
        "is_home": payload["is_home"],
        "goals": payload.get("goals", 0),
        "possession": payload.get("possession"),
        "shots": payload.get("shots"),
        "shots_on_target": payload.get("shots_on_target"),
        "passes": payload.get("passes"),
        "pass_accuracy": payload.get("pass_accuracy"),
        "corners": payload.get("corners"),
        "fouls": payload.get("fouls"),
        "yellow_cards": payload.get("yellow_cards"),
        "red_cards": payload.get("red_cards"),
        "xg": payload.get("xg"),
    }

    created = False
    if not stats:
        stats = TeamMatchStats(**fields)
        db.add(stats)
        created = True
    else:
        for key, value in fields.items():
            setattr(stats, key, value)

    return created


def upsert_player_stats(db, match: Match, payload: dict) -> bool:
    player = payload.get("player")
    if not player:
        raise ValueError("Player not resolved for stats payload")

    stats = db.execute(
        select(PlayerMatchStats).where(
            PlayerMatchStats.match_id == match.id,
            PlayerMatchStats.player_id == player.id,
        )
    ).scalar_one_or_none()

    fields = {
        "match_id": match.id,
        "player_id": player.id,
        "team_id": payload["team_id"],
        "minutes": payload.get("minutes_played"),
        "goals": payload.get("goals"),
        "assists": payload.get("assists"),
        "shots": payload.get("shots"),
        "shots_on_target": payload.get("shots_on_target"),
        "key_passes": payload.get("key_passes"),
        "passes": payload.get("passes"),
        "pass_accuracy": payload.get("pass_accuracy"),
        "tackles": payload.get("tackles"),
        "interceptions": payload.get("interceptions"),
        "duels_won": payload.get("duels_won"),
        "fouls_committed": payload.get("fouls_committed"),
        "yellow_cards": payload.get("yellow_cards"),
        "red_cards": payload.get("red_cards"),
        "rating": payload.get("rating"),
        "saves": payload.get("saves"),
        "xg": payload.get("xg"),
        "xa": payload.get("xa"),
    }

    created = False
    if not stats:
        stats = PlayerMatchStats(**fields)
        db.add(stats)
        created = True
    else:
        for key, value in fields.items():
            setattr(stats, key, value)

    return created


def _to_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_player_stats(stats_payload: dict) -> dict:
    return {
        "minutes_played": _to_int(stats_payload.get("minutes_played")),
        "goals": _to_int(stats_payload.get("goals")),
        "assists": _to_int(stats_payload.get("assists")),
        "shots": _to_int(stats_payload.get("shots")),
        "shots_on_target": _to_int(stats_payload.get("shots_on_target")),
        "key_passes": _to_int(stats_payload.get("key_passes")),
        "passes": _to_int(stats_payload.get("passes")),
        "pass_accuracy": _to_float(stats_payload.get("pass_accuracy")),
        "tackles": _to_int(stats_payload.get("tackles")),
        "interceptions": _to_int(stats_payload.get("interceptions")),
        "duels_won": _to_int(stats_payload.get("duels_won")),
        "fouls_committed": _to_int(stats_payload.get("fouls_committed")),
        "yellow_cards": _to_int(stats_payload.get("yellow_cards")),
        "red_cards": _to_int(stats_payload.get("red_cards")),
        "rating": _to_float(stats_payload.get("rating")),
        "saves": _to_int(stats_payload.get("saves")),
        "xg": _to_float(stats_payload.get("xg")),
        "xa": _to_float(stats_payload.get("xa")),
    }
