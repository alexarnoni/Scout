"""
Busca logos dos times e salva no banco via team.logo_url.

Fontes (em ordem de prioridade):
  1. ESPN scoreboard — logo oficial, sem autenticação
  2. SportDB search API — fallback para times não encontrados na ESPN

Estratégia de match ESPN:
  1. Nome exato
  2. Nome normalizado (sem acentos, hífens, sufixos de estado)
  3. Similaridade via difflib (cutoff configurável)

Uso:
    python -m scripts.fetch_logos              # usa o ano atual
    python -m scripts.fetch_logos --season 2025
    python -m scripts.fetch_logos --season 2025 --cutoff 0.7
"""
from __future__ import annotations

import argparse
import re
import time
import sys
import os
import unicodedata
from datetime import date
from difflib import get_close_matches

import httpx
import requests
from sqlalchemy import select, extract

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.db import SessionLocal
from app.models.team import Team
from app.models.match import Match

SPORTDB_API_KEY = os.getenv("SPORTDB_API_KEY", "")
SPORTDB_BASE = "https://api.sportdb.dev"

ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/bra.1/scoreboard"

_STATE_SUFFIXES = r"\b(RJ|SP|MG|PR|SC|RS|BA|CE|GO|PE|RN|PA|AM|ES|MT|MS|AL|SE|PI|MA|TO|RO|AC|AP|RR|DF)\b"


def normalize(name: str) -> str:
    """Remove acentos, sufixos de estado, pontuação e lowercase."""
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ASCII", "ignore").decode()
    name = re.sub(_STATE_SUFFIXES, "", name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-z0-9]", "", name.lower())
    return name.strip()


def get_match_dates(year: int) -> list[str]:
    """Retorna datas únicas (YYYYMMDD) das partidas do ano no banco."""
    db = SessionLocal()
    try:
        rows = db.execute(
            select(Match.match_date_time).where(
                extract("year", Match.match_date_time) == year
            )
        ).scalars().all()
    finally:
        db.close()

    return sorted({dt.strftime("%Y%m%d") for dt in rows})


def fetch_logo_map(dates: list[str]) -> dict[str, str]:
    """Retorna {team_name: logo_url} varrendo os scoreboards das datas fornecidas."""
    logo_map: dict[str, str] = {}

    for d in dates:
        try:
            resp = requests.get(ESPN_SCOREBOARD_URL, params={"dates": d}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"[WARN] Falha ao buscar scoreboard {d}: {exc}")
            time.sleep(1)
            continue

        for event in data.get("events", []):
            for competitor in event.get("competitions", [{}])[0].get("competitors", []):
                team = competitor.get("team", {})
                name = team.get("displayName") or team.get("name")
                logo = team.get("logo")
                if name and logo and name not in logo_map:
                    logo_map[name] = logo

        print(f"[INFO] {d}: {len(logo_map)} times mapeados até agora")
        time.sleep(1)

    return logo_map


def resolve_logo(
    team_name: str,
    logo_map: dict[str, str],
    norm_map: dict[str, str],
    espn_norm_keys: list[str],
    cutoff: float,
) -> tuple[str | None, str]:
    """
    Tenta casar team_name com logo_map em 3 etapas.
    Retorna (logo_url | None, método usado).
    """
    # 1. Exato
    if team_name in logo_map:
        return logo_map[team_name], "exact"

    # 2. Normalizado
    key = normalize(team_name)
    if key in norm_map:
        return norm_map[key], "normalized"

    # 3. Similaridade
    matches = get_close_matches(key, espn_norm_keys, n=1, cutoff=cutoff)
    if matches:
        return norm_map[matches[0]], f"fuzzy({matches[0]})"

    return None, "not_found"


def fetch_logo_sportdb(team_name: str) -> str | None:
    """Busca logo no SportDB search API. Requer SPORTDB_API_KEY."""
    if not SPORTDB_API_KEY:
        return None
    try:
        r = httpx.get(
            f"{SPORTDB_BASE}/api/flashscore/search",
            params={"q": team_name, "type": "team"},
            headers={"X-API-Key": SPORTDB_API_KEY},
            timeout=10,
        )
        r.raise_for_status()
        for result in r.json().get("results", []):
            sport = result.get("sport", {}).get("name", "")
            country = result.get("country", {}).get("name", "")
            images = result.get("images", [])
            if sport == "Soccer" and country == "Brazil" and images:
                return images[0]
    except Exception as exc:
        print(f"[WARN] SportDB falhou para {team_name!r}: {exc}")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Popula logo_url dos times via ESPN.")
    parser.add_argument(
        "--season",
        type=int,
        default=date.today().year,
        metavar="YYYY",
        help="Ano da temporada (padrão: ano atual)",
    )
    parser.add_argument(
        "--cutoff",
        type=float,
        default=0.75,
        metavar="0-1",
        help="Cutoff de similaridade para fuzzy match (padrão: 0.75)",
    )
    args = parser.parse_args()

    print(f"[INFO] Buscando datas de partidas para o ano {args.season}...")
    dates = get_match_dates(args.season)

    if not dates:
        print(f"[ERROR] Nenhuma partida encontrada no banco para {args.season}. Abortando.")
        sys.exit(1)

    print(f"[INFO] {len(dates)} datas encontradas.")
    logo_map = fetch_logo_map(dates)
    print(f"[INFO] Total de logos encontrados na ESPN: {len(logo_map)}")

    # Pré-computa mapa normalizado da ESPN: {norm_key: logo_url}
    norm_map: dict[str, str] = {}
    for espn_name, url in logo_map.items():
        k = normalize(espn_name)
        if k not in norm_map:
            norm_map[k] = url
    espn_norm_keys = list(norm_map.keys())

    db = SessionLocal()
    try:
        teams = db.execute(select(Team)).scalars().all()
        updated = 0
        not_found: list[str] = []

        for team in teams:
            logo_url, method = resolve_logo(
                team.name, logo_map, norm_map, espn_norm_keys, args.cutoff
            )
            if logo_url:
                team.logo_url = logo_url
                updated += 1
                if method != "exact":
                    print(f"[INFO] {team.name!r} → ESPN match via {method}")
            else:
                # Fallback: SportDB
                logo_url = fetch_logo_sportdb(team.name)
                if logo_url:
                    team.logo_url = logo_url
                    updated += 1
                    print(f"[INFO] {team.name!r} → SportDB fallback")
                else:
                    not_found.append(team.name)

        db.commit()
    finally:
        db.close()

    for name in not_found:
        print(f"[WARN] Logo não encontrado: {name}")

    print(f"\nResumo: updated={updated} not_found={len(not_found)}")


if __name__ == "__main__":
    main()
