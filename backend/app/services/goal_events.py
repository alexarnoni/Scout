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
from app.providers.sportdb import get_match_details, get_match_playerstats

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


# ---------------------------------------------------------------------------
# Ingestão completa de stats por partida (novo formato FlashScore)
# ---------------------------------------------------------------------------

def _extract_incident_events(data: dict) -> list[dict]:
    """Extrai eventos no formato incidentType/incidentPlayerId do FlashScore."""
    if isinstance(data, list):
        return data
    for key in ("incidents", "events", "data"):
        val = data.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            for subkey in ("incidents", "events"):
                sub = val.get(subkey)
                if isinstance(sub, list):
                    return sub
    return []


def _extract_player_name(event: dict) -> str | None:
    """Tenta extrair o nome de um jogador a partir de campos comuns do evento."""
    for field in (
        "incidentPlayerName",
        "playerName",
        "participantName",
        "name",
    ):
        val = event.get(field)
        if val and isinstance(val, str):
            return val.strip()
    return None


def _resolve_or_create_player(
    sportdb_id: str,
    event: dict,
    fallback_team_id: int,
    db: Session,
    player_cache: dict[str, Player | None],
) -> Player | None:
    """Retorna Player existente ou cria um novo se não encontrado."""
    player = _resolve_player(sportdb_id, db, player_cache)
    if player is not None:
        return player

    name = _extract_player_name(event)
    if not name:
        logger.warning(
            "Jogador sportdb_id=%s não encontrado e sem nome no evento — ignorando",
            sportdb_id,
        )
        return None

    player = Player(
        name=name,
        team_id=fallback_team_id,
        external_ids={"sportdb": sportdb_id},
    )
    db.add(player)
    db.flush()  # gera o id
    player_cache[sportdb_id] = player
    logger.info("Jogador criado: id=%s name=%s sportdb_id=%s", player.id, name, sportdb_id)
    return player


def ingest_match_player_stats(db: Session, match: Match) -> dict[str, int]:
    """
    Ingestão completa de stats de jogadores para uma partida via SportDB.

    A) GET /match/{id}/details?with_events=true — gols, assistências, cartões, substituições
    B) GET /match/{id}/playerstats             — ratings por jogador

    Calcula minutos jogados a partir dos eventos de substituição.
    Cria jogadores no banco se não existirem (usando nome do evento).
    Faz upsert em player_match_stats por (match_id, player_id).

    Idempotente: chamar duas vezes produz o mesmo estado.
    """
    import httpx

    sportdb_event_id = match.sportdb_event_id
    if not sportdb_event_id:
        raise ValueError(f"Match {match.id} não tem sportdb_event_id preenchido")

    # ── A) Eventos ──────────────────────────────────────────────────────────
    try:
        details = get_match_details(sportdb_event_id)
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(
            f"SportDB error (details) for event {sportdb_event_id}: {exc.response.status_code}"
        ) from exc

    events = _extract_incident_events(details)

    # Acumulador por sportdb_player_id
    # {sportdb_id: {"goals", "assists", "yellow_cards", "red_cards",
    #               "sub_out_minute", "sub_in_minute", "_event": dict}}
    accumulator: dict[str, dict] = defaultdict(
        lambda: {
            "goals": 0,
            "assists": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "sub_out_minute": None,
            "sub_in_minute": None,
            "_event": {},
        }
    )

    _TYPE_GOAL = "3"
    _TYPE_ASSIST = "8"
    _TYPE_YELLOW = "1"
    _TYPE_RED = "2"
    _TYPE_SUB_OUT = "6"
    _TYPE_SUB_IN = "7"

    for event in events:
        raw_type = event.get("incidentType")
        if raw_type is None:
            continue

        types: list[str] = (
            [str(t) for t in raw_type]
            if isinstance(raw_type, list)
            else [str(raw_type)]
        )

        raw_ids = event.get("incidentPlayerId") or []
        player_ids: list[str] = (
            [str(pid) for pid in raw_ids]
            if isinstance(raw_ids, list)
            else [str(raw_ids)]
        )

        incident_time = (
            event.get("incidentTime")
            or event.get("time")
            or event.get("minute")
        )
        minute = _parse_minute(incident_time)

        for i, t in enumerate(types):
            if i >= len(player_ids):
                break
            pid = player_ids[i]
            if not pid:
                continue

            acc = accumulator[pid]
            # Store event reference for name extraction (first seen wins)
            if not acc["_event"]:
                acc["_event"] = event

            if t == _TYPE_GOAL:
                acc["goals"] += 1
            elif t == _TYPE_ASSIST:
                acc["assists"] += 1
            elif t == _TYPE_YELLOW:
                acc["yellow_cards"] += 1
            elif t == _TYPE_RED:
                acc["red_cards"] += 1
            elif t == _TYPE_SUB_OUT:
                if minute is not None:
                    acc["sub_out_minute"] = minute
            elif t == _TYPE_SUB_IN:
                if minute is not None:
                    acc["sub_in_minute"] = minute

    # ── B) Ratings ──────────────────────────────────────────────────────────
    ratings: dict[str, float] = {}
    try:
        playerstats = get_match_playerstats(sportdb_event_id)
        for entry in (playerstats if isinstance(playerstats, list) else []):
            pid = str(entry.get("id") or "")
            if not pid:
                continue
            rating_field = entry.get("rating")
            val: float | None = None
            if isinstance(rating_field, dict):
                raw_val = rating_field.get("numericValue")
                if raw_val is not None:
                    try:
                        val = float(raw_val)
                    except (TypeError, ValueError):
                        pass
            elif isinstance(rating_field, (int, float)):
                val = float(rating_field)
            if val is not None:
                ratings[pid] = val
    except Exception:
        logger.warning(
            "Não foi possível obter playerstats para event %s — ratings ignorados",
            sportdb_event_id,
        )

    # Adicionar players com rating mas sem eventos de incidente
    for pid in ratings:
        if pid not in accumulator:
            accumulator[pid]  # defaultdict cria entrada vazia

    # ── C) Resolver e upsert ─────────────────────────────────────────────────
    player_cache: dict[str, Player | None] = {}
    fallback_team_id = match.home_team_id

    players_processed = 0
    goals_ingested = 0
    assists_ingested = 0
    cards_ingested = 0
    created_players = 0

    for sportdb_id, stats in accumulator.items():
        player = _resolve_or_create_player(
            sportdb_id,
            stats["_event"],
            fallback_team_id,
            db,
            player_cache,
        )
        if player is None:
            continue

        if player_cache.get(sportdb_id) is not None and stats["_event"] == {}:
            # Player só tem rating, sem eventos — ainda processamos
            pass

        # Calcular minutos
        sub_out = stats["sub_out_minute"]
        sub_in = stats["sub_in_minute"]
        if sub_out is not None:
            minutes = sub_out
        elif sub_in is not None:
            minutes = max(0, 90 - sub_in)
        else:
            # Apareceu em algum evento (gol/cartão) sem sub — assumir titular completo
            minutes = 90

        team_id = player.team_id

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

        current.goals = max(current.goals or 0, stats["goals"])
        current.assists = max(current.assists or 0, stats["assists"])
        current.yellow_cards = max(current.yellow_cards or 0, stats["yellow_cards"])
        current.red_cards = max(current.red_cards or 0, stats["red_cards"])
        current.minutes = minutes
        if sportdb_id in ratings:
            current.rating = ratings[sportdb_id]

        goals_ingested += stats["goals"]
        assists_ingested += stats["assists"]
        cards_ingested += stats["yellow_cards"] + stats["red_cards"]
        players_processed += 1

    db.flush()

    return {
        "players_processed": players_processed,
        "goals_ingested": goals_ingested,
        "assists_ingested": assists_ingested,
        "cards_ingested": cards_ingested,
        "created_players": created_players,
    }
