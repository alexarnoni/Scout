"""World Cup 2026 router — exposes 6 endpoints under /worldcup prefix."""

from fastapi import APIRouter, HTTPException, Query

from app.providers.sportdb_worldcup import (
    get_copa_standings,
    get_copa_fixtures,
    get_copa_results,
    get_match_stats,
    get_match_lineups,
    get_team_squad,
)

router = APIRouter(prefix="/worldcup", tags=["worldcup"])


@router.get("/standings")
def standings():
    try:
        data = get_copa_standings()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar standings da Copa do Mundo",
        )
    return data


@router.get("/fixtures")
def fixtures(page: int = Query(default=1, ge=1)):
    try:
        data = get_copa_fixtures(page)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar fixtures da Copa do Mundo",
        )
    return data


@router.get("/results")
def results(page: int = Query(default=1, ge=1)):
    try:
        data = get_copa_results(page)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar results da Copa do Mundo",
        )
    return data


@router.get("/match/{event_id}/stats")
def match_stats(event_id: str):
    try:
        data = get_match_stats(event_id)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar match stats da Copa do Mundo",
        )
    return data


@router.get("/match/{event_id}/lineups")
def match_lineups(event_id: str):
    try:
        data = get_match_lineups(event_id)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar lineups da Copa do Mundo",
        )
    return data


@router.get("/team/{team_slug}/{team_id}/squad")
def team_squad(team_slug: str, team_id: str):
    try:
        raw = get_team_squad(team_slug, team_id)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Erro ao buscar squad da Copa do Mundo",
        )

    # Squad normalization: rename countryName → club
    squad_entries = raw.get("squad", [])
    normalized_entries = []
    for entry in squad_entries:
        players = entry.get("players", [])
        normalized_players = []
        for p in players:
            normalized_players.append({
                "id": p.get("id"),
                "slug": p.get("slug"),
                "firstName": p.get("firstName"),
                "lastName": p.get("lastName"),
                "jerseyNumber": p.get("jerseyNumber"),
                "position": p.get("position"),
                "club": p.get("countryName"),  # rename countryName → club, None if absent
                "link": p.get("link"),
            })
        normalized_entries.append({**entry, "players": normalized_players})

    if not normalized_entries:
        return {
            "id": raw.get("id"),
            "teamName": raw.get("teamName"),
            "teamLogo": raw.get("teamLogo"),
            "squad": [],
            "message": "Convocação ainda não divulgada pela FIFA",
        }

    return {**raw, "squad": normalized_entries}
