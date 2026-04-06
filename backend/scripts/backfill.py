from __future__ import annotations

import argparse
import datetime
import logging
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ROOT_ENV)

from app.core.config import DATABASE_URL
from app.providers import ESPNProvider
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# All persistence logic lives in sync_date — import, don't duplicate
from scripts.sync_date import build_sportdb_index, process_date_matches

logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _date_range(start: datetime.date, end: datetime.date):
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += datetime.timedelta(days=1)


def _add_counters(total: dict, day: dict) -> None:
    """Accumulate day counters into total in-place."""
    for key in total:
        total[key] += day.get(key, 0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill ESPN matches for a date range."
    )
    parser.add_argument("--competition-id", type=int, required=True)
    parser.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--include-players",
        action="store_true",
        # Explicit flag (not nargs="?" like sync_date) — cleaner for a batch script
        help="Sync player stats",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        help="Seconds to wait between dates (default: 3.0)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        date_from = datetime.date.fromisoformat(args.date_from)
        date_to = datetime.date.fromisoformat(args.date_to)
    except ValueError as exc:
        parser.error(f"Invalid date format: {exc}. Expected YYYY-MM-DD.")

    if date_from > date_to:
        parser.error(f"--date-from ({date_from}) must be <= --date-to ({date_to})")

    dates = list(_date_range(date_from, date_to))
    logger.info(
        "Backfill: %d day(s) from %s to %s | competition_id=%s | include_players=%s | dry_run=%s",
        len(dates), date_from, date_to, args.competition_id, args.include_players, args.dry_run,
    )

    provider = ESPNProvider()

    total_counters = {
        "inserted_matches": 0,
        "updated_matches": 0,
        "inserted_stats": 0,
        "updated_stats": 0,
        "skipped_stats": 0,
        "inserted_players": 0,
        "inserted_player_stats": 0,
        "updated_player_stats": 0,
    }
    days_processed = 0
    days_with_matches = 0

    for i, date in enumerate(dates):
        # Delay before every request except the first — courtesy to ESPN
        if i > 0:
            time.sleep(args.delay)

        logger.info("[%d/%d] Processing %s…", i + 1, len(dates), date)

        matches = provider.fetch_matches_by_date(date)

        if not matches:
            # Not an error — competition may have no games on this date
            logger.info("No matches on %s, skipping.", date)
            days_processed += 1
            continue

        days_with_matches += 1
        logger.info("%d match(es) found on %s", len(matches), date)

        with SessionLocal() as db:
            try:
                sportdb_index = build_sportdb_index(date)
                day_counters = process_date_matches(
                    db,
                    args.competition_id,
                    matches,
                    args.include_players,
                    args.verbose,
                    sportdb_index=sportdb_index,
                )
            except Exception as exc:
                # Roll back this day and continue — don't abort the whole backfill.
                # The operator can re-run just this date with sync_date.py.
                db.rollback()
                logger.error("Error processing %s — rolled back, continuing: %s", date, exc)
                days_processed += 1
                continue

            if args.dry_run:
                db.rollback()
            else:
                db.commit()

        _add_counters(total_counters, day_counters)
        days_processed += 1

    print(
        f"\nBackfill complete: "
        f"days_processed={days_processed} "
        f"days_with_matches={days_with_matches} "
        f"matches_inserted={total_counters['inserted_matches']} "
        f"matches_updated={total_counters['updated_matches']} "
        f"stats_inserted={total_counters['inserted_stats']} "
        f"stats_updated={total_counters['updated_stats']} "
        f"stats_skipped={total_counters['skipped_stats']} "
        f"players_created={total_counters['inserted_players']} "
        f"player_stats_inserted={total_counters['inserted_player_stats']} "
        f"player_stats_updated={total_counters['updated_player_stats']}"
    )


if __name__ == "__main__":
    main()
