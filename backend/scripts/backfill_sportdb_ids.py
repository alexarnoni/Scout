"""
Backfill sportdb_event_id nas partidas do banco a partir dos resultados da temporada SportDB.

Para cada resultado retornado pela API, cruza home/away slugs + data com as partidas
no banco e preenche matches.sportdb_event_id onde ainda estiver vazio.

Uso:
    cd backend
    python -m scripts.backfill_sportdb_ids --competition-id 1
    python -m scripts.backfill_sportdb_ids --competition-id 1 --dry-run
    python -m scripts.backfill_sportdb_ids --competition-id 1 --season 2026 --page 1
"""
from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ROOT_ENV)

from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker, joinedload  # noqa: E402

from app.core.config import DATABASE_URL  # noqa: E402
from app.models.match import Match  # noqa: E402
from app.models.team import Team  # noqa: E402
from app.providers.sportdb import get_season_results, TEAM_SLUG_MAP  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Reverse map: slug → team name  (e.g. "flamengo-rj" → "Flamengo")
_SLUG_TO_NAME: dict[str, str] = {v: k for k, v in TEAM_SLUG_MAP.items()}


def _parse_sportdb_date(ts) -> list[str]:
    """Converte startTimestamp (epoch int, epoch string ou string ISO) para lista de datas.

    Retorna até 2 datas (UTC e UTC-3/BRT) para cobrir partidas noturnas onde a
    diferença de fuso pode mudar o dia calendário.
    """
    if not ts:
        return []
    ts_str = str(ts).strip()
    # Epoch numérico (int, float, ou string puramente numérica)
    if isinstance(ts, (int, float)) or ts_str.lstrip("-").isdigit():
        dt_utc = datetime.datetime.utcfromtimestamp(int(float(ts_str)))
    else:
        try:
            dt_utc = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if dt_utc.tzinfo is not None:
                dt_utc = dt_utc.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        except ValueError:
            dt_utc = datetime.datetime.strptime(ts_str[:10], "%Y-%m-%d")

    dt_brt = dt_utc - datetime.timedelta(hours=3)
    dates = list({dt_utc.date().isoformat(), dt_brt.date().isoformat()})
    return dates


def backfill(competition_id: int, season: str, page: int, dry_run: bool) -> None:
    logger.info(
        "Buscando resultados SportDB season=%s page=%s ...", season, page
    )
    results = get_season_results(season=season, page=page)
    logger.info("%d resultados retornados pela API", len(results))

    with SessionLocal() as db:
        # Pré-carregar todos os times do banco: {name → id}
        all_teams = db.execute(select(Team)).scalars().all()
        name_to_id: dict[str, int] = {t.name: t.id for t in all_teams}
        logger.debug("Times no banco: %d", len(name_to_id))

        # Carregar partidas sem sportdb_event_id (com equipes)
        matches_stmt = (
            select(Match)
            .where(
                Match.sportdb_event_id.is_(None),
                Match.competition_id == competition_id,
            )
            .options(joinedload(Match.home_team), joinedload(Match.away_team))
        )
        pending_matches = db.execute(matches_stmt).unique().scalars().all()
        logger.info("%d partida(s) sem sportdb_event_id no banco", len(pending_matches))

        # Índice das partidas do banco: {(home_team_id, away_team_id, date_iso) → Match}
        match_index: dict[tuple[int, int, str], Match] = {}
        for m in pending_matches:
            if m.match_date_time is None:
                continue
            dt = m.match_date_time
            if dt.tzinfo is not None:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            date_iso = dt.date().isoformat()
            match_index[(m.home_team_id, m.away_team_id, date_iso)] = m

        updated = 0
        skipped_no_slug = 0
        skipped_no_match = 0

        for result in results:
            event_id = result.get("eventId") or result.get("id")
            if not event_id:
                continue

            home_slug = result.get("homeParticipantNameUrl", "")
            away_slug = result.get("awayParticipantNameUrl", "")
            home_name = _SLUG_TO_NAME.get(home_slug)
            away_name = _SLUG_TO_NAME.get(away_slug)

            if not home_name or not away_name:
                skipped_no_slug += 1
                logger.debug(
                    "Slug sem mapeamento — home=%s away=%s (eventId=%s)",
                    home_slug, away_slug, event_id,
                )
                continue

            home_team_id = name_to_id.get(home_name)
            away_team_id = name_to_id.get(away_name)

            if not home_team_id or not away_team_id:
                skipped_no_slug += 1
                logger.debug(
                    "Time não encontrado no banco — home=%s away=%s",
                    home_name, away_name,
                )
                continue

            candidate_dates = _parse_sportdb_date(
                result.get("startTimestamp") or result.get("startTime")
            )

            match = None
            for date_iso in candidate_dates:
                match = match_index.get((home_team_id, away_team_id, date_iso))
                if match:
                    break

            if match is None:
                skipped_no_match += 1
                logger.debug(
                    "Partida não encontrada no banco — home=%s away=%s dates=%s",
                    home_name, away_name, candidate_dates,
                )
                continue

            logger.info(
                "match_id=%s  %s vs %s  →  sportdb_event_id=%s",
                match.id, home_name, away_name, event_id,
            )
            match.sportdb_event_id = str(event_id)
            updated += 1

        logger.info(
            "Resumo: atualizadas=%d sem_slug=%d sem_partida=%d dry_run=%s",
            updated, skipped_no_slug, skipped_no_match, dry_run,
        )

        if dry_run:
            logger.info("Dry-run: nenhuma alteração persistida.")
            db.rollback()
        else:
            db.commit()
            logger.info("Commit realizado.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill matches.sportdb_event_id via SportDB results API."
    )
    parser.add_argument("--competition-id", type=int, default=1)
    parser.add_argument("--season", default="2026")
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    backfill(
        competition_id=args.competition_id,
        season=args.season,
        page=args.page,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
