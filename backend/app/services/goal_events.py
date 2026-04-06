"""
Serviço de ingestão de eventos de gol via SportDB.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.match import Match
from app.models.player import Player
from app.models.player_match_stats import PlayerMatchStats
from app.models.team import Team
from app.providers.sportdb import get_match_details

logger = logging.getLogger(__name__)

_EXCLUDED_GOAL_TYPES = {"DISALLOWED", "CANCELLED", "ANNULLED"}


def _extract_events(data: dict) -> list[dict]:
    """Extrai lista de eventos do JSON retornado pela API."""
    if isinstance(data, list):
        return data
    if "events" in data:
        return data["events"] or []
    if "data" in data and isinstance(data["data"], dict):
        return data["data"].get("events") or []
    return []


def _normalize_event_type(event: dict) -> str:
    raw = (
        event.get("type")
        or event.get("eventType")
        or event.get("event_type")
        or ""
    )
    return str(raw).strip().upper().replace("-", "_").replace(" ", "_")


def _is_goal_event(event: dict) -> bool:
    event_type = _normalize_event_type(event)
    if not event_type:
        return False
    if any(token in event_type for token in _EXCLUDED_GOAL_TYPES):
        return False
    return "GOAL" in event_type


def _parse_minute(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().replace("'", "")
    if not text:
        return None

    if "+" in text:
        base, extra = text.split("+", 1)
        if base.isdigit() and extra.isdigit():
            return int(base) + int(extra)

    match = re.search(r"\d+", text)
    if match:
        return int(match.group(0))
    return None


def ingest_match_events(
    sportdb_event_id: str,
    match_id: int,
    db: Session,
) -> dict[str, int]:
    """
    Busca eventos de gol via SportDB e faz upsert em player_match_stats.
    Retorna {"goals_ingested": N, "assists_ingested": M}.

    A operação é idempotente: chamar duas vezes produz o mesmo estado.
    """
    import httpx

    try:
        data = get_match_details(sportdb_event_id)
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(
            f"SportDB error for event {sportdb_event_id}: {exc.response.status_code}"
        ) from exc

    events = _extract_events(data)

    # Acumula goals e assists por (match_id, player_id)
    # Estrutura:
    # {player_id: {"goals": int, "assists": int, "team_id": int | None, "first_goal_minute": int | None}}
    accumulator: dict[int, dict] = defaultdict(
        lambda: {"goals": 0, "assists": 0, "team_id": None, "first_goal_minute": None}
    )
    player_cache: dict[str, Player | None] = {}
    team_cache: dict[str, Team | None] = {}

    match = db.execute(select(Match).where(Match.id == match_id)).scalar_one_or_none()
    if not match:
        raise ValueError(f"Match {match_id} not found in database")

    for event in events:
        if not _is_goal_event(event):
            continue

        participant_id = (
            event.get("participantId")
            or event.get("playerId")
            or event.get("scorerParticipantId")
        )
        assist_participant_id = (
            event.get("assistParticipantId")
            or event.get("assistPlayerId")
            or event.get("assistId")
        )
        team_id_external = (
            event.get("teamId")
            or event.get("participantTeamId")
            or event.get("teamParticipantId")
        )
        minute = _parse_minute(
            event.get("minute")
            or event.get("eventMinute")
            or event.get("time")
            or event.get("eventTime")
        )

        # Resolver jogador que marcou
        if participant_id is not None:
            scorer = _resolve_player(str(participant_id), db, player_cache)
            if scorer is None:
                logger.warning(
                    "Player with sportdb id %s not found, skipping goal event",
                    participant_id,
                )
            else:
                accumulator[scorer.id]["goals"] += 1
                if accumulator[scorer.id]["team_id"] is None:
                    accumulator[scorer.id]["team_id"] = _resolve_team_id(
                        team_id_external,
                        scorer,
                        db,
                        team_cache,
                    )
                previous_minute = accumulator[scorer.id]["first_goal_minute"]
                if minute is not None and (
                    previous_minute is None or minute < previous_minute
                ):
                    accumulator[scorer.id]["first_goal_minute"] = minute

        # Resolver assistente (opcional)
        if assist_participant_id is not None:
            assistant = _resolve_player(str(assist_participant_id), db, player_cache)
            if assistant is None:
                logger.warning(
                    "Assist player with sportdb id %s not found, skipping assist",
                    assist_participant_id,
                )
            else:
                accumulator[assistant.id]["assists"] += 1
                if accumulator[assistant.id]["team_id"] is None:
                    accumulator[assistant.id]["team_id"] = _resolve_team_id(
                        team_id_external,
                        assistant,
                        db,
                        team_cache,
                    )

    goals_ingested = 0
    assists_ingested = 0

    for player_id, stats in accumulator.items():
        player = db.execute(select(Player).where(Player.id == player_id)).scalar_one_or_none()
        if player is None:
            continue

        team_id = stats["team_id"] or player.team_id
        current = db.execute(
            select(PlayerMatchStats).where(
                PlayerMatchStats.match_id == match.id,
                PlayerMatchStats.player_id == player.id,
            )
        ).scalar_one_or_none()

        if current is None:
            current = PlayerMatchStats(
                match_id=match.id,
                player_id=player.id,
                team_id=team_id,
            )
            db.add(current)
        else:
            current.team_id = team_id

        # Não apagar outras métricas já persistidas (minutes/shots/etc):
        # apenas ajusta goals/assists por evento.
        current.goals = max(current.goals or 0, stats["goals"])
        current.assists = max(current.assists or 0, stats["assists"])

        if stats["first_goal_minute"] is not None:
            logger.debug(
                "Goal minute parsed for match=%s player=%s minute=%s",
                match.id,
                player.id,
                stats["first_goal_minute"],
            )

        goals_ingested += stats["goals"]
        assists_ingested += stats["assists"]

    db.flush()

    return {"goals_ingested": goals_ingested, "assists_ingested": assists_ingested}


def _resolve_player(
    sportdb_id: str,
    db: Session,
    cache: dict[str, Player | None],
) -> Player | None:
    """Busca jogador pelo external_ids["sportdb"] == sportdb_id."""
    if sportdb_id in cache:
        return cache[sportdb_id]

    cache[sportdb_id] = db.execute(
        select(Player).where(Player.external_ids[("sportdb")].as_string() == sportdb_id)
    ).scalar_one_or_none()
    return cache[sportdb_id]


def _resolve_team_id(
    external_team_id,
    player: Player,
    db: Session,
    cache: dict[str, Team | None],
) -> int:
    if external_team_id is None:
        return player.team_id

    external_team_id = str(external_team_id)
    if external_team_id in cache:
        team = cache[external_team_id]
        return team.id if team else player.team_id

    cache[external_team_id] = db.execute(
        select(Team).where(Team.external_ids[("sportdb")].as_string() == external_team_id)
    ).scalar_one_or_none()
    team = cache[external_team_id]
    return team.id if team else player.team_id
