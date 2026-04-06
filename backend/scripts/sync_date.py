from __future__ import annotations

import argparse
import datetime
import logging
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ROOT_ENV)

from app.core.config import DATABASE_URL
from app.models import Player, Team
from app.providers import ESPNProvider
from app.providers.sportdb import get_season_results as sportdb_get_season_results, TEAM_SLUG_MAP

from app.services.persistence import (
    extract_xg,
    parse_player_stats,
    upsert_match,
    upsert_player_stats,
    upsert_team_stats,
)

logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

_SOURCE = "espn"


def build_sportdb_index(sync_date: datetime.date) -> dict[tuple[str, str], str]:
    """Constrói índice {(home_slug, away_slug): eventId} para a data informada.

    Cruza resultados do SportDB com partidas ESPN pelo par de slugs de time e data.
    Retorna dict vazio se a API falhar (não interrompe o fluxo principal).
    """
    try:
        results = sportdb_get_season_results()
        date_str = sync_date.isoformat()  # "YYYY-MM-DD"
        index: dict[tuple[str, str], str] = {}
        for match in results:
            # startTimestamp pode ser epoch ou string ISO
            ts = match.get("startTimestamp") or match.get("startTime") or ""
            if isinstance(ts, int):
                match_date = datetime.datetime.utcfromtimestamp(ts).date().isoformat()
            else:
                match_date = str(ts)[:10]
            if match_date != date_str:
                continue
            home_slug = match.get("homeParticipantNameUrl", "")
            away_slug = match.get("awayParticipantNameUrl", "")
            event_id = match.get("eventId") or match.get("id")
            if home_slug and away_slug and event_id:
                index[(home_slug, away_slug)] = str(event_id)
        return index
    except Exception:
        logger.warning("Não foi possível construir índice SportDB — sportdb_event_id não será preenchido")
        return {}


def process_date_matches(
    db,
    competition_id: int,
    matches: list[dict],
    include_players: bool,
    verbose: bool,
    sportdb_index: dict | None = None,
) -> dict:
    """Persist a list of ESPN match payloads for one date into an open db session.

    Extracted from main() so backfill.py can call it in a loop without
    duplicating any logic. Commit/rollback is the caller's responsibility.

    Returns a counters dict with keys:
        inserted_matches, updated_matches, inserted_stats, updated_stats,
        skipped_stats, inserted_players, inserted_player_stats, updated_player_stats
    """
    counters = {
        "inserted_matches": 0,
        "updated_matches": 0,
        "inserted_stats": 0,
        "updated_stats": 0,
        "skipped_stats": 0,
        "inserted_players": 0,
        "inserted_player_stats": 0,
        "updated_player_stats": 0,
    }

    for match_payload in matches:
        # ESPN doesn't provide reliable round numbers — set explicitly to None
        match_payload["round"] = None

        # Tentar resolver sportdb_event_id cruzando com o índice SportDB
        if sportdb_index:
            home_slug = TEAM_SLUG_MAP.get(match_payload.get("home_team", ""))
            away_slug = TEAM_SLUG_MAP.get(match_payload.get("away_team", ""))
            if home_slug and away_slug:
                sportdb_event_id = sportdb_index.get((home_slug, away_slug))
                if sportdb_event_id:
                    match_payload["sportdb_event_id"] = sportdb_event_id

        match, created = upsert_match(db, competition_id, _SOURCE, match_payload)
        if created:
            counters["inserted_matches"] += 1
        else:
            counters["updated_matches"] += 1

        if match.status != "finished":
            counters["skipped_stats"] += 1
            logger.debug(
                "Skipping stats for unfinished match %s (status=%s)",
                match_payload.get("external_id"),
                match.status,
            )
            continue

        for stats_payload in match_payload.get("team_stats", []):
            team = db.execute(
                select(Team).where(
                    Team.competition_id == competition_id,
                    Team.name == stats_payload["team_name"],
                )
            ).scalar_one_or_none()
            if not team:
                raise ValueError(f"Team not found: {stats_payload['team_name']!r}")

            stats_payload["team_id"] = team.id
            stats_payload["xg"] = extract_xg(stats_payload)

            if verbose:
                if stats_payload["xg"] is not None:
                    logger.debug(
                        "xg found: match_id=%s team_id=%s xg=%s",
                        match.id, team.id, stats_payload["xg"],
                    )
                else:
                    logger.debug(
                        "xg missing: match_id=%s team_id=%s", match.id, team.id
                    )

            stats_created = upsert_team_stats(db, match, stats_payload)
            if stats_created:
                counters["inserted_stats"] += 1
            else:
                counters["updated_stats"] += 1

        if include_players:
            for player_payload in match_payload.get("player_stats", []):
                team = db.execute(
                    select(Team).where(
                        Team.competition_id == competition_id,
                        Team.name == player_payload["team_name"],
                    )
                ).scalar_one_or_none()
                if not team:
                    raise ValueError(
                        f"Team not found: {player_payload['team_name']!r}"
                    )

                player = None
                external_id = player_payload.get("player_external_id")

                # 1. Lookup by ESPN ID — O(1) after first sync
                if external_id:
                    player = db.execute(
                        select(Player).where(
                            Player.external_ids[_SOURCE].as_string() == external_id
                        )
                    ).scalar_one_or_none()

                if not player:
                    # 2. Fallback: lookup by name within team (first sync only)
                    # Strip whitespace to handle ESPN trailing-space quirks
                    clean_name = player_payload["player_name"].strip()
                    player = db.execute(
                        select(Player).where(
                            Player.team_id == team.id,
                            Player.name == clean_name,
                        )
                    ).scalar_one_or_none()

                    if player and external_id:
                        # Found by name — backfill ESPN ID so next sync uses it directly
                        ids = player.external_ids or {}
                        ids[_SOURCE] = external_id
                        player.external_ids = ids
                        logger.debug(
                            "Backfilled espn id=%s for player %r (id=%s)",
                            external_id, player.name, player.id,
                        )

                if not player and external_id:
                    # 3. Not in DB at all — create from ESPN data
                    jersey = player_payload.get("jersey")
                    player = Player(
                        team_id=team.id,
                        name=player_payload["player_name"].strip(),
                        position=player_payload.get("position"),
                        shirt_number=int(jersey) if jersey and jersey.isdigit() else None,
                        external_ids={_SOURCE: external_id},
                    )
                    db.add(player)
                    db.flush()  # get player.id before using in stats
                    counters["inserted_players"] += 1
                    logger.debug(
                        "Created player %r (espn_id=%s, team_id=%s)",
                        player.name, external_id, team.id,
                    )

                if not player:
                    # No external_id and not found by name — nothing we can do
                    logger.warning(
                        "Player not found and no ESPN id — skipping: "
                        "match_id=%s team_id=%s name=%r",
                        match.id, team.id, player_payload["player_name"],
                    )
                    continue

                parsed_stats = parse_player_stats(player_payload.get("stats", {}))
                parsed_stats["team_id"] = team.id
                parsed_stats["player"] = player

                created_player = upsert_player_stats(db, match, parsed_stats)
                if created_player:
                    counters["inserted_player_stats"] += 1
                else:
                    counters["updated_player_stats"] += 1

    return counters


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ESPN matches for a date and persist to the database."
    )
    parser.add_argument("--date", required=True, help="Date to sync in YYYY-MM-DD format")
    parser.add_argument("--competition-id", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--include-players",
        nargs="?",
        const="true",
        default="false",
        help="Sync player stats (default: false)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    include_players = str(args.include_players).lower() in {"1", "true", "yes", "y"}

    try:
        sync_date = datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"Invalid date format: {args.date!r}. Expected YYYY-MM-DD.")

    provider = ESPNProvider()
    matches = provider.fetch_matches_by_date(sync_date)
    logger.info("Provider returned %d match(es) for %s", len(matches), sync_date)

    sportdb_index = build_sportdb_index(sync_date)
    logger.info("SportDB index built: %d entries for %s", len(sportdb_index), sync_date)

    with SessionLocal() as db:
        counters = process_date_matches(
            db, args.competition_id, matches, include_players, args.verbose,
            sportdb_index=sportdb_index,
        )

        if args.dry_run:
            db.rollback()
            logger.info("Dry run — rolled back all changes")
        else:
            db.commit()

    print(
        "Summary: "
        f"matches_inserted={counters['inserted_matches']} "
        f"matches_updated={counters['updated_matches']} "
        f"stats_inserted={counters['inserted_stats']} "
        f"stats_updated={counters['updated_stats']} "
        f"stats_skipped={counters['skipped_stats']} "
        f"players_created={counters['inserted_players']} "
        f"player_stats_inserted={counters['inserted_player_stats']} "
        f"player_stats_updated={counters['updated_player_stats']}"
    )


if __name__ == "__main__":
    main()
