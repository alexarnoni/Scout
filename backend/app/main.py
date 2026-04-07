from contextlib import asynccontextmanager
import asyncio
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, joinedload

from app.api.router import api_router
from app.core.db import get_db
from app.models import (
    Competition,
    Match,
    Player,
    PlayerMatchStats,
    Roster,
    Staff,
    Team,
    TeamMatchStats,
)
from app.schemas import (
    CompetitionOut,
    PlayerOut,
    MatchOut,
    PlayerDetailOut,
    RosterOut,
    StaffOut,
    TeamSquadOut,
    TeamMatchStatsOut,
    TeamOut,
)
from app.schemas.analytics import (
    TeamAnalyticsSummary,
    TeamRadar,
    TeamTimeSeriesPoint,
    TopScorerItem,
    TopScorersResponse,
)
from app.schemas.player_analytics import (
    PlayerAnalyticsSummary,
    PlayerRadar,
    PlayerTimeSeriesPoint,
)
from app.services.team_analytics import (
    get_last_matches,
    get_team_averages,
    get_team_radar as build_team_radar,
    get_team_timeseries,
    get_team_trend,
)
from app.services.player_analytics import (
    get_last_matches as get_player_last_matches,
    get_player_averages,
    get_player_radar,
    get_player_timeseries,
)
from app.services.scout import get_scout_ranking
from app.providers.sportdb_scout import SPORTDB_POSITION_GROUPS
from app.providers.sportdb import (
    get_last_match_event_id,
    get_match_lineup,
    get_team_slug,
    get_season_results,
    get_standings,
    get_match_stats as fetch_match_stats,
    get_team_season_averages,
    get_player_profile,
    get_player_market_value,
    get_season_fixtures,
)
from app.schemas.scout import PlayerScoutCard, ScoutRanking

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    logger.info("sportdb pre-fetch starting...")
    try:
        await asyncio.gather(
            asyncio.to_thread(get_standings, season="2026"),
            asyncio.to_thread(get_season_results, season="2026", page=1),
            asyncio.to_thread(get_season_fixtures, season="2026", page=1),
        )
        logger.info("sportdb pre-fetch completed")
    except Exception:
        logger.warning("sportdb pre-fetch failed", exc_info=True)
    yield


app = FastAPI(title="Scout API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://scout.alexarnoni.com"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router)


def get_competition_or_404(db: Session, competition_id: int) -> Competition:
    competition = db.execute(
        select(Competition).where(Competition.id == competition_id)
    ).scalar_one_or_none()
    if not competition:
        raise HTTPException(status_code=404, detail="Competition not found")
    return competition


def get_team_or_404(db: Session, team_id: int) -> Team:
    team = db.execute(select(Team).where(Team.id == team_id)).scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    return team


def get_team_in_competition_or_404(
    db: Session, team_id: int, competition_id: int
) -> Team:
    team = db.execute(
        select(Team).where(
            Team.id == team_id,
            Team.competition_id == competition_id,
        )
    ).scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found in competition")
    return team


def get_player_or_404(db: Session, player_id: int) -> Player:
    player = db.execute(
        select(Player).where(Player.id == player_id)
    ).scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


def get_player_with_team_or_404(db: Session, player_id: int) -> Player:
    player = db.execute(
        select(Player)
        .where(Player.id == player_id)
        .options(joinedload(Player.team))
    ).scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


def get_player_in_competition_or_404(
    db: Session, player_id: int, competition_id: int
) -> Player:
    player = db.execute(
        select(Player)
        .join(Team, Team.id == Player.team_id)
        .where(Player.id == player_id, Team.competition_id == competition_id)
    ).scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found in competition")
    return player


def get_match_or_404(db: Session, match_id: int) -> Match:
    match = db.execute(
        select(Match)
        .where(Match.id == match_id)
        .options(joinedload(Match.home_team), joinedload(Match.away_team))
    ).scalar_one_or_none()
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    return match


def _aggregate_team_player_stats(team_id: str) -> list[dict]:
    from app.providers.sportdb_scout import get_match_player_stats, get_season_results

    results = get_season_results()
    team_matches = [
        m for m in results
        if m.get("eventStage") == "FINISHED"
        and team_id in (m.get("homeParticipantIds", ""), m.get("awayParticipantIds", ""))
    ]
    if not team_matches:
        return []

    first_match = team_matches[0]
    team_name = (
        first_match.get("homeName")
        if first_match.get("homeParticipantIds") == team_id
        else first_match.get("awayName")
    )
    if not team_name:
        return []

    players: dict[str, dict] = {}

    for match in team_matches:
        event_id = match.get("eventId", "")
        if not event_id:
            continue
        try:
            players_raw = get_match_player_stats(event_id)
        except Exception:
            continue

        team_players = [p for p in players_raw if p.get("team_name") == team_name]
        for p in team_players:
            player_id = str(p.get("player_id", ""))
            if not player_id:
                continue

            if player_id not in players:
                players[player_id] = {
                    "player_id": player_id,
                    "name": str(p.get("player_name", "")),
                    "player_name": str(p.get("player_name", "")),
                    "goals": 0,
                    "assists": 0,
                    "yellow_cards": 0,
                    "total_minutes": 0,
                    "matches_played": 0,
                    "_ratings": [],
                }

            acc = players[player_id]
            acc["goals"] += int(p.get("goals", 0) or 0)
            acc["assists"] += int(p.get("assists", 0) or 0)
            acc["yellow_cards"] += int(p.get("yellow_cards", 0) or 0)
            acc["total_minutes"] += int(p.get("minutes", 0) or 0)
            acc["matches_played"] += 1

            rating = p.get("rating")
            if rating is not None:
                rating_value = float(rating or 0.0)
                if rating_value > 0:
                    acc["_ratings"].append(rating_value)

    aggregated: list[dict] = []
    for acc in players.values():
        ratings = acc.pop("_ratings", [])
        acc["avg_rating"] = (sum(ratings) / len(ratings)) if ratings else 0.0
        aggregated.append(acc)

    return aggregated


@app.get("/teams/{team_id}/top_scorers", response_model=TopScorersResponse)
def get_top_scorers(team_id: str) -> TopScorersResponse:
    players = _aggregate_team_player_stats(team_id)
    ranked = sorted(
        players,
        key=lambda p: (
            -int(p.get("goals", 0) or 0),
            -int(p.get("assists", 0) or 0),
            str(p.get("player_name", "")).lower(),
        ),
    )[:5]

    top_scorers: list[TopScorerItem] = []
    for player in ranked:
        try:
            player_id = int(str(player.get("player_id", "0")))
        except ValueError:
            continue
        top_scorers.append(
            TopScorerItem(
                player_id=player_id,
                name=str(player.get("player_name", "")),
                goals=int(player.get("goals", 0) or 0),
                assists=int(player.get("assists", 0) or 0),
                matches_played=int(player.get("matches_played", 0) or 0),
            )
        )

    return TopScorersResponse(team_id=team_id, top_scorers=top_scorers)


@app.get("/teams/{team_id}/top_assists", response_model=list[dict])
def get_top_assists(team_id: str) -> list[dict]:
    players = _aggregate_team_player_stats(team_id)
    ranked = sorted(
        players,
        key=lambda p: (
            -int(p.get("assists", 0) or 0),
            str(p.get("player_name", "")).lower(),
        ),
    )[:5]
    return [
        {
            "name": str(player.get("player_name", "")),
            "value": int(player.get("assists", 0) or 0),
            "matches_played": int(player.get("matches_played", 0) or 0),
        }
        for player in ranked
    ]


@app.get("/teams/{team_id}/top_ratings", response_model=list[dict])
def get_top_ratings(team_id: str) -> list[dict]:
    players = _aggregate_team_player_stats(team_id)
    rated_players = [p for p in players if p.get("avg_rating") is not None]
    ranked = sorted(
        rated_players,
        key=lambda p: (
            -float(p.get("avg_rating", 0.0) or 0.0),
            str(p.get("player_name", "")).lower(),
        ),
    )[:5]
    return [
        {
            "name": str(player.get("player_name", "")),
            "value": round(float(player.get("avg_rating", 0.0) or 0.0), 2),
            "matches_played": int(player.get("matches_played", 0) or 0),
        }
        for player in ranked
    ]


@app.get("/teams/{team_id}/top_minutes", response_model=list[dict])
def get_top_minutes(team_id: str) -> list[dict]:
    players = _aggregate_team_player_stats(team_id)
    ranked = sorted(
        players,
        key=lambda p: (
            -int(p.get("total_minutes", 0) or 0),
            str(p.get("player_name", "")).lower(),
        ),
    )[:5]
    return [
        {
            "name": str(player.get("player_name", "")),
            "value": int(player.get("total_minutes", 0) or 0),
            "matches_played": int(player.get("matches_played", 0) or 0),
        }
        for player in ranked
    ]


@app.get("/teams/{team_id}/top_yellow_cards", response_model=list[dict])
def get_top_yellow_cards(team_id: str) -> list[dict]:
    players = _aggregate_team_player_stats(team_id)
    ranked = sorted(
        players,
        key=lambda p: (
            -int(p.get("yellow_cards", 0) or 0),
            str(p.get("player_name", "")).lower(),
        ),
    )[:5]
    return [
        {
            "name": str(player.get("player_name", "")),
            "value": int(player.get("yellow_cards", 0) or 0),
            "matches_played": int(player.get("matches_played", 0) or 0),
        }
        for player in ranked
    ]


@app.get("/teams/{team_id}/last_lineup", response_model=list[PlayerOut])
def get_team_last_lineup(
    team_id: int,
    competition_id: int,
    db: Session = Depends(get_db),
) -> list[Player]:
    get_team_or_404(db, team_id)

    last_match = db.execute(
        select(Match)
        .where(
            or_(Match.home_team_id == team_id, Match.away_team_id == team_id),
            Match.competition_id == competition_id,
            Match.status == 'finished',
        )
        .order_by(Match.match_date_time.desc())
        .limit(1)
    ).scalar_one_or_none()

    if not last_match:
        return []

    player_stats = db.execute(
        select(PlayerMatchStats)
        .where(
            PlayerMatchStats.match_id == last_match.id,
            PlayerMatchStats.team_id == team_id,
            PlayerMatchStats.minutes > 0,
        )
    ).scalars().all()

    player_ids = [ps.player_id for ps in player_stats]

    if not player_ids:
        return []

    players = db.execute(
        select(Player)
        .where(Player.id.in_(player_ids))
        .order_by(Player.name)
    ).scalars().all()

    return players


@app.get("/competitions", response_model=list[CompetitionOut])
def list_competitions(db: Session = Depends(get_db)) -> list[Competition]:
    competitions = db.execute(
        select(Competition).order_by(Competition.name)
    ).scalars().all()
    return competitions


@app.get("/competitions/{competition_id}/teams", response_model=list[TeamOut])
def list_competition_teams(
    competition_id: int, db: Session = Depends(get_db)
) -> list[Team]:
    get_competition_or_404(db, competition_id)
    teams = db.execute(
        select(Team)
        .where(Team.competition_id == competition_id)
        .order_by(Team.name)
    ).scalars().all()
    return teams


@app.get("/teams/{team_id}", response_model=TeamOut)
def get_team_detail(team_id: int, db: Session = Depends(get_db)) -> Team:
    return get_team_or_404(db, team_id)


@app.get("/teams/{team_id}/roster", response_model=list[RosterOut])
def get_team_roster(
    team_id: int, db: Session = Depends(get_db)
) -> list[Roster]:
    get_team_or_404(db, team_id)
    rosters = db.execute(
        select(Roster)
        .where(Roster.team_id == team_id)
        .options(joinedload(Roster.player))
    ).scalars().all()
    return rosters


@app.get("/teams/{team_id}/staff", response_model=list[StaffOut])
def get_team_staff(team_id: int, db: Session = Depends(get_db)) -> list[Staff]:
    get_team_or_404(db, team_id)
    staff_members = db.execute(
        select(Staff).where(Staff.team_id == team_id).order_by(Staff.name)
    ).scalars().all()
    return staff_members


@app.get("/players/{player_id}", response_model=PlayerDetailOut)
def get_player_detail(player_id: int, db: Session = Depends(get_db)) -> Player:
    return get_player_with_team_or_404(db, player_id)


@app.get(
    "/players/{player_id}/analytics/summary", response_model=PlayerAnalyticsSummary
)
def get_player_analytics_summary(
    player_id: int,
    competition_id: int,
    window: int = 5,
    db: Session = Depends(get_db),
) -> dict:
    get_player_in_competition_or_404(db, player_id, competition_id)
    last_matches = get_player_last_matches(db, player_id, competition_id, window)
    averages = get_player_averages(db, player_id, competition_id, window)
    return {
        "player_id": player_id,
        "competition_id": competition_id,
        "window": window,
        "averages": averages,
        "last_matches": last_matches,
    }


@app.get("/players/{player_id}/analytics/radar", response_model=PlayerRadar)
def get_player_analytics_radar(
    player_id: int,
    competition_id: int,
    window: int = 5,
    min_matches: int = 3,
    min_players: int = 30,
    db: Session = Depends(get_db),
) -> dict:
    get_player_in_competition_or_404(db, player_id, competition_id)
    radar = get_player_radar(
        db, player_id, competition_id, window, min_matches, min_players
    )
    return {
        "player_id": player_id,
        "competition_id": competition_id,
        "window": window,
        "min_matches": min_matches,
        "min_players": min_players,
        "eligible_players": radar["eligible_players"],
        "metrics": radar["metrics"],
        "note": radar["note"],
    }


@app.get(
    "/players/{player_id}/analytics/timeseries",
    response_model=list[PlayerTimeSeriesPoint],
)
def get_player_analytics_timeseries(
    player_id: int,
    competition_id: int,
    db: Session = Depends(get_db),
) -> list[dict]:
    get_player_in_competition_or_404(db, player_id, competition_id)
    return get_player_timeseries(db, player_id, competition_id)


@app.get("/matches", response_model=list[MatchOut])
def list_matches(
    competition_id: int, db: Session = Depends(get_db)
) -> list[Match]:
    matches = db.execute(
        select(Match)
        .where(Match.competition_id == competition_id)
        .options(joinedload(Match.home_team), joinedload(Match.away_team))
        .order_by(Match.match_date_time)
    ).scalars().all()
    return matches


@app.get("/matches/{match_id}", response_model=MatchOut)
def get_match_detail(match_id: int, db: Session = Depends(get_db)) -> Match:
    return get_match_or_404(db, match_id)


@app.get("/matches/{match_id}/stats", response_model=list[TeamMatchStatsOut])
def get_match_stats(
    match_id: int, db: Session = Depends(get_db)
) -> list[TeamMatchStats]:
    get_match_or_404(db, match_id)
    stats = db.execute(
        select(TeamMatchStats)
        .where(TeamMatchStats.match_id == match_id)
        .order_by(TeamMatchStats.is_home.desc())
    ).scalars().all()
    return stats


@app.get("/teams/{team_id}/matches")
def get_team_matches(team_id: str) -> list[dict]:
    from app.providers.sportdb_scout import get_season_results
    results = get_season_results()
    matches = []
    for m in results:
        home_id = m.get("homeParticipantIds", "")
        away_id = m.get("awayParticipantIds", "")
        if team_id not in (home_id, away_id):
            continue
        is_home = home_id == team_id
        goals_for = int(m.get("homeFullTimeScore") or 0) if is_home else int(m.get("awayFullTimeScore") or 0)
        goals_against = int(m.get("awayFullTimeScore") or 0) if is_home else int(m.get("homeFullTimeScore") or 0)
        if goals_for > goals_against:
            result = "W"
        elif goals_for < goals_against:
            result = "L"
        else:
            result = "D"
        matches.append({
            "opponent": m.get("awayName") if is_home else m.get("homeName"),
            "goals_for": goals_for,
            "goals_against": goals_against,
            "result": result,
            "is_home": is_home,
            "round": m.get("round", ""),
            "date": m.get("startDateTimeUtc", ""),
        })
    matches.sort(key=lambda x: x.get("date", ""))
    return matches


def _parse_flashscore_participant_url(participant_url: str) -> tuple[str, str] | None:
    clean = (participant_url or "").replace("/player/", "").strip("/")
    if not clean:
        return None
    parts = clean.split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _extract_matches_played_2026(profile: dict) -> int | None:
    league_2026 = next(
        (
            c
            for c in profile.get("careers", {}).get("league", [])
            if c.get("season") == "2026" and c.get("competitionSlug") == "serie-a-betano"
        ),
        None,
    )
    if not league_2026:
        return None

    matches_played = next(
        (s.get("value") for s in league_2026.get("stats", []) if s.get("name") == "Matches Played"),
        None,
    )
    if matches_played is None:
        return None

    try:
        return int(matches_played)
    except (TypeError, ValueError):
        try:
            return int(float(str(matches_played).replace(",", ".")))
        except (TypeError, ValueError):
            return None


def _get_db_matches_played_2026_by_participant_id(
    db: Session,
    participant_ids: set[str],
) -> dict[str, int]:
    if not participant_ids:
        return {}

    sportdb_id = Player.external_ids[("sportdb")].as_string()
    rows = db.execute(
        select(
            sportdb_id.label("sportdb_id"),
            func.count(PlayerMatchStats.id).label("matches_played"),
        )
        .join(Player, Player.id == PlayerMatchStats.player_id)
        .join(Match, Match.id == PlayerMatchStats.match_id)
        .join(Competition, Competition.id == Match.competition_id)
        .where(
            sportdb_id.in_(participant_ids),
            or_(
                Competition.season == "2026",
                Competition.name.ilike("%2026%"),
            ),
        )
        .group_by(sportdb_id)
    ).all()

    return {
        str(row.sportdb_id): int(row.matches_played or 0)
        for row in rows
        if row.sportdb_id is not None
    }


@app.get("/teams/{team_id}/squad")
def get_team_squad(team_id: str) -> dict:
    from app.providers.sportdb_scout import get_season_results, get_match_player_stats

    results = get_season_results()

    team_matches = [
        m for m in results
        if team_id in (m.get("homeParticipantIds", ""), m.get("awayParticipantIds", ""))
    ]
    if not team_matches:
        return {"team": {}, "players": [], "staff": []}

    last_match = sorted(team_matches, key=lambda x: x.get("startDateTimeUtc", ""))[-1]
    event_id = last_match.get("eventId", "")

    try:
        players_raw = get_match_player_stats(event_id)
    except Exception:
        return {"team": {}, "players": [], "staff": []}

    team_players = [p for p in players_raw if p["team_id"] == team_id]

    players = [
        {
            "id": p["player_id"],
            "name": p["player_name"],
            "position": p["position"],
            "shirt_number": None,
            "is_substitute": p["is_substitute"],
        }
        for p in team_players
    ]

    team_name = last_match.get("homeName") if last_match.get("homeParticipantIds") == team_id else last_match.get("awayName")

    return {
        "team": {"id": team_id, "name": team_name},
        "players": players,
        "staff": [],
    }


@app.get("/teams/{team_id}/next_fixture")
def get_next_fixture(team_id: str) -> dict:
    import datetime
    try:
        fixtures = get_season_fixtures()
    except Exception:
        raise HTTPException(status_code=404, detail="Sem próximo jogo")
    now_ts = int(datetime.datetime.now().timestamp())
    for match in fixtures:
        home = match.get("homeParticipantIds", "")
        away = match.get("awayParticipantIds", "")
        start = int(match.get("startUtime", 0))
        if team_id in (home, away) and start > now_ts:
            dt = datetime.datetime.fromtimestamp(start, tz=datetime.timezone(datetime.timedelta(hours=-3)))
            is_home = home == team_id
            team_logo_hash = match.get("homeLogo", "") if is_home else match.get("awayLogo", "")
            team_logo = f"https://static.flashscore.com/res/image/data/{team_logo_hash}" if team_logo_hash else ""
            return {
                "event_id": match.get("eventId"),
                "date": dt.strftime("%a, %d %b · %H:%M"),
                "competition": "Brasileirão Série A",
                "home_name": match.get("homeFirstName", ""),
                "away_name": match.get("awayFirstName", ""),
                "home_logo": f"https://static.flashscore.com/res/image/data/{match.get('homeLogo', '')}" if match.get("homeLogo") else "",
                "away_logo": f"https://static.flashscore.com/res/image/data/{match.get('awayLogo', '')}" if match.get("awayLogo") else "",
                "team_logo": team_logo,
                "venue": "",
            }
    raise HTTPException(status_code=404, detail="Sem próximo jogo")


@app.get("/teams/{team_id}/radar")
def get_team_radar(
    team_id: int, window: str = "season", db: Session = Depends(get_db)
) -> dict:
    get_team_or_404(db, team_id)
    if window not in {"season", "last5"}:
        raise HTTPException(status_code=400, detail="Invalid window")

    query = (
        select(TeamMatchStats, Match.match_date_time)
        .join(Match, Match.id == TeamMatchStats.match_id)
        .where(TeamMatchStats.team_id == team_id)
        .order_by(Match.match_date_time.desc())
    )
    if window == "last5":
        query = query.limit(5)

    rows = db.execute(query).all()
    stats = [row[0] for row in rows]

    def avg(values: list[float | int]) -> float | None:
        return sum(values) / len(values) if values else None

    def normalize(value: float | None, min_val: float, max_val: float) -> float | None:
        if value is None:
            return None
        if max_val <= min_val:
            return None
        scaled = (value - min_val) / (max_val - min_val) * 100
        return max(0.0, min(100.0, scaled))

    metrics_raw = {
        "goals": avg([s.goals for s in stats if s.goals is not None]),
        "shots": avg([s.shots for s in stats if s.shots is not None]),
        "shots_on_target": avg(
            [s.shots_on_target for s in stats if s.shots_on_target is not None]
        ),
        "possession": avg([s.possession for s in stats if s.possession is not None]),
        "pass_accuracy": avg(
            [s.pass_accuracy for s in stats if s.pass_accuracy is not None]
        ),
        "corners": avg([s.corners for s in stats if s.corners is not None]),
        "fouls": avg([s.fouls for s in stats if s.fouls is not None]),
        "xg": avg([s.xg for s in stats if s.xg is not None]),
    }

    ranges = {
        "goals": (0.0, 5.0),
        "shots": (0.0, 30.0),
        "shots_on_target": (0.0, 15.0),
        "possession": (0.0, 100.0),
        "pass_accuracy": (0.0, 100.0),
        "corners": (0.0, 15.0),
        "fouls": (0.0, 30.0),
        "xg": (0.0, 5.0),
    }

    metrics = {
        key: normalize(metrics_raw[key], *ranges[key]) for key in ranges
    }

    return {"team_id": team_id, "window": window, "metrics": metrics}


@app.get("/teams/{team_id}/analytics/summary")
def get_team_analytics_summary(team_id: str) -> dict:
    from .providers.sportdb_scout import get_season_results
    results = get_season_results()
    team_matches = [
        m for m in results
        if team_id in (m.get("homeParticipantIds", ""), m.get("awayParticipantIds", ""))
        and m.get("eventStage") == "FINISHED"
    ]
    wins = draws = losses = gf = ga = 0
    for m in team_matches:
        is_home = m.get("homeParticipantIds") == team_id
        goals_for = int(m.get("homeFullTimeScore") or 0) if is_home else int(m.get("awayFullTimeScore") or 0)
        goals_against = int(m.get("awayFullTimeScore") or 0) if is_home else int(m.get("homeFullTimeScore") or 0)
        gf += goals_for
        ga += goals_against
        if goals_for > goals_against:
            wins += 1
        elif goals_for == goals_against:
            draws += 1
        else:
            losses += 1
    return {
        "played": len(team_matches),
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "goals_for": gf,
        "goals_against": ga,
        "averages": {},
    }


@app.get("/teams/{team_id}/analytics/radar", response_model=TeamRadar)
def get_team_analytics_radar(
    team_id: int,
    competition_id: int,
    window: int = 5,
    min_matches: int = 3,
    min_teams: int = 6,
    db: Session = Depends(get_db),
) -> dict:
    get_team_in_competition_or_404(db, team_id, competition_id)
    radar = build_team_radar(
        db, team_id, competition_id, window, min_matches, min_teams
    )
    return {
        "team_id": team_id,
        "competition_id": competition_id,
        "window": window,
        "min_matches": min_matches,
        "min_teams": min_teams,
        "eligible_teams": radar["eligible_teams"],
        "metrics": radar["metrics"],
        "note": radar["note"],
    }


@app.get("/teams/{team_id}/analytics/timeseries", response_model=list[TeamTimeSeriesPoint])
def get_team_analytics_timeseries(
    team_id: int,
    competition_id: int,
    db: Session = Depends(get_db),
) -> list[dict]:
    get_team_in_competition_or_404(db, team_id, competition_id)
    return get_team_timeseries(db, team_id, competition_id)


VALID_POSITIONS = {"Goleiro", "Defensor", "Meio-campo", "Atacante"}


@app.get("/standings")
def get_competition_standings(db: Session = Depends(get_db)) -> list[dict]:
    from app.providers.sportdb import TEAM_SLUG_MAP

    data = get_standings()

    # invert TEAM_SLUG_MAP: slug → team_name  (e.g. "palmeiras" → "Palmeiras")
    slug_to_name: dict[str, str] = {v: k for k, v in TEAM_SLUG_MAP.items()}

    # fetch all teams from DB in one query
    team_names = list(slug_to_name.values())
    teams_in_db = db.execute(
        select(Team).where(Team.name.in_(team_names))
    ).scalars().all()
    name_to_logo: dict[str, str | None] = {t.name: t.logo_url for t in teams_in_db}

    def resolve_logo(team_slug: str) -> str | None:
        name = slug_to_name.get(team_slug)
        if not name:
            return None
        return name_to_logo.get(name)

    return [
        {
            "rank": t["rank"],
            "teamId": t["teamId"],
            "teamName": t["teamName"],
            "teamSlug": t["teamSlug"],
            "points": int(t["points"]),
            "matches": int(t["matches"]),
            "wins": int(t["wins"]),
            "draws": int(t["draws"]),
            "losses": int(t["lossesRegular"]),
            "goals": t["goals"],
            "goalDiff": int(t["goalDiff"]),
            "rankClass": t.get("rankClass", ""),
            "form": [e["eventSymbol"] for e in t.get("events", []) if e.get("eventType") != "upcoming"][:5],
            "logo_url": resolve_logo(t.get("teamSlug", "")),
        }
        for t in data
    ]


@app.get("/teams/{team_id}/flashscore_lineup")
def get_flashscore_lineup(
    team_id: str,
    db: Session = Depends(get_db),
) -> dict:
    slug = team_id
    if not slug:
        raise HTTPException(status_code=404, detail=f"Slug não encontrado: {team_id}")

    results = get_season_results()
    event_id = None
    is_home = True
    for match in reversed(results):
        home_id = match.get("homeParticipantIds", "")
        away_id = match.get("awayParticipantIds", "")
        if home_id == team_id:
            event_id = match.get("eventId")
            is_home = True
            break
        elif away_id == team_id:
            event_id = match.get("eventId")
            is_home = False
            break

    if not event_id:
        raise HTTPException(status_code=404, detail="Nenhuma partida encontrada")

    lineup = get_match_lineup(event_id)
    starters_group = lineup.get("starters", {})
    subs_group = lineup.get("subs", {})

    side = "home" if is_home else "away"

    players = starters_group.get(side, [])
    bench_players = subs_group.get(side, [])

    matches_played_2026_by_id: dict[str, int] = {}
    missing_participant_ids: set[str] = set()

    for p in players:
        participant_id = str(p.get("participantId", "")).strip()
        if not participant_id:
            continue

        parsed = _parse_flashscore_participant_url(str(p.get("participantUrl", "")))
        matches_played = None
        if parsed:
            slug, pid = parsed
            try:
                profile = get_player_profile(slug, pid)
                matches_played = _extract_matches_played_2026(profile)
            except Exception:
                matches_played = None

        if matches_played is None:
            missing_participant_ids.add(participant_id)
        else:
            matches_played_2026_by_id[participant_id] = matches_played

    if missing_participant_ids:
        matches_played_2026_by_id.update(
            _get_db_matches_played_2026_by_participant_id(db, missing_participant_ids)
        )

    for p in players:
        participant_id = str(p.get("participantId", "")).strip()
        p["matches_played_2026"] = matches_played_2026_by_id.get(participant_id)

    formation = ""
    if players:
        formation = (players[0].get("formation", "") or "").replace("1-", "")

    try:
        raw_stats = fetch_match_stats(event_id)
        match_period = next((p for p in raw_stats if p["period"] == "Match"), None)
        stats = {}
        if match_period:
            STAT_MAP = {
                "Ball possession": "possession",
                "Total shots": "shots",
                "Shots on target": "shots_on_target",
                "Expected goals (xG)": "xg",
                "Corner kicks": "corners",
                "Passes": "passes",
                "Fouls": "fouls",
                "Yellow cards": "yellow_cards",
            }
            for s in match_period["stats"]:
                key = STAT_MAP.get(s["statName"])
                if key and key not in stats:
                    stats[key] = {
                        "home": s["homeValue"],
                        "away": s["awayValue"],
                    }
    except Exception:
        stats = {}

    return {
        "event_id": event_id,
        "side": side,
        "formation": formation,
        "starters": players,
        "bench": bench_players,
        "match_stats": stats,
        "is_home": is_home,
    }


@app.get("/teams/{team_id}/season_averages")
def get_team_season_averages_endpoint(
    team_id: str,
) -> dict:
    slug = team_id
    if not slug:
        raise HTTPException(status_code=404, detail="Slug não encontrado")
    return get_team_season_averages(slug)


@app.get("/player/flashscore/{player_slug}/{player_id}")
def get_flashscore_player(player_slug: str, player_id: str) -> dict:
    try:
        data = get_player_profile(player_slug, player_id)
        league_2026 = next(
            (c for c in data.get("careers", {}).get("league", [])
             if c.get("season") == "2026" and c.get("competitionSlug") == "serie-a-betano"),
            None
        )
        stats_2026 = {}
        if league_2026:
            for s in league_2026.get("stats", []):
                stats_2026[s["name"]] = s["value"]
        return {
            "name": f"{data.get('firstName', '')} {data.get('lastName', '')}".strip(),
            "photo": data.get("photo", ""),
            "market_value": data.get("marketValue", ""),
            "dob": data.get("dob", ""),
            "position": data.get("position", ""),
            "country": data.get("countryName", ""),
            "status": data.get("playerStatus", ""),
            "rating_2026": stats_2026.get("Rating", 0),
            "goals_2026": stats_2026.get("Goals Scored", 0),
            "assists_2026": stats_2026.get("Assists", 0),
            "matches_2026": stats_2026.get("Matches Played", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/scout/ranking", response_model=list[ScoutRanking])
def scout_ranking(
    position: str,
    season: str = "2026",
    min_minutes: int = 180,
) -> list[dict]:
    if position not in VALID_POSITIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid position. Must be one of: {', '.join(sorted(VALID_POSITIONS))}",
        )
    return get_scout_ranking(position, min_minutes, season)


@app.get("/scout/moneyball")
def scout_moneyball(
    position: str,
    season: str = "2026",
    min_minutes: int = 180,
) -> list[dict]:
    import re
    if position not in VALID_POSITIONS:
        raise HTTPException(status_code=400, detail="Invalid position")
    ranking = get_scout_ranking(position, min_minutes, season)
    result = []
    for p in ranking[:20]:
        mv_str = get_player_market_value(p["player_name"])
        mv_num = None
        if mv_str:
            match = re.search(r'[\d.]+', mv_str.replace(',', '.'))
            if match:
                val = float(match.group())
                if 'k' in mv_str.lower():
                    val /= 1000
                mv_num = val
        moneyball = round(p["score"] / mv_num, 2) if mv_num and mv_num > 0 else None
        result.append({**p, "market_value": mv_str, "market_value_m": mv_num, "moneyball_score": moneyball})
    result.sort(key=lambda x: x["moneyball_score"] or 0, reverse=True)
    return result


@app.get("/scout/player/{player_id}", response_model=PlayerScoutCard)
def scout_player_card(
    player_id: str,
    season: str = "2026",
    min_minutes: int = 180,
) -> dict:
    # Busca o jogador nos dados agregados da temporada
    from app.providers.sportdb_scout import get_player_season_stats
    all_players = get_player_season_stats(season, min_minutes)
    player_data = next((p for p in all_players if p["player_id"] == player_id), None)
    if player_data is None:
        raise HTTPException(
            status_code=404,
            detail="Player not found in ranking (insufficient minutes or no stats).",
        )
    position_group = player_data.get("position_group")
    if position_group is None:
        raise HTTPException(
            status_code=422,
            detail=f"Player position '{player_data.get('position')}' is not rankable.",
        )
    ranking = get_scout_ranking(position_group, min_minutes, season)
    entry = next((r for r in ranking if r["player_id"] == player_id), None)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail="Player not found in ranking (insufficient minutes or no stats).",
        )
    rank = next(i + 1 for i, r in enumerate(ranking) if r["player_id"] == player_id)
    return {**entry, "rank": rank}
