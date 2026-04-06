"""
Testes unitários para app.providers.sportdb_scout
Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
"""
import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.providers.sportdb_scout import (
    SPORTDB_POSITION_GROUPS,
    _cache,
    _merge_lineup_stats,
    _p90,
    get_player_season_stats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_lineup(players_home: list[dict], players_away: list[dict] | None = None) -> list[dict]:
    """Monta estrutura mínima de lineups_data."""
    return [
        {
            "group": "Starting XI",
            "home": players_home,
            "away": players_away or [],
        }
    ]


def _make_teams(home_id: str = "t1", away_id: str = "t2") -> list[dict]:
    """Monta estrutura mínima de teams_data."""
    return [
        {"id": home_id, "name": "Home FC", "side": "home"},
        {"id": away_id, "name": "Away FC", "side": "away"},
    ]


def _player(pid: str, name: str = "Player", position: str = "MID", minutes: int | None = None) -> dict:
    p = {"participantId": pid, "participantName": name, "positionKey": position}
    if minutes is not None:
        p["minutesPlayed"] = minutes
    return p


# ---------------------------------------------------------------------------
# _p90
# ---------------------------------------------------------------------------

class TestP90:
    def test_minutes_zero_returns_zero(self):
        assert _p90(10.0, 0) == 0.0

    def test_minutes_negative_returns_zero(self):
        assert _p90(5.0, -1) == 0.0

    def test_correct_calculation(self):
        result = _p90(9.0, 90.0)
        assert abs(result - 9.0) < 1e-9

    def test_partial_minutes(self):
        result = _p90(1.0, 45.0)
        assert abs(result - 2.0) < 1e-9


# ---------------------------------------------------------------------------
# _merge_lineup_stats
# ---------------------------------------------------------------------------

class TestMergeLineupStats:
    def test_player_without_stats_gets_zeros(self):
        lineup = _make_lineup([_player("p1")])
        merged = _merge_lineup_stats(lineup, [], _make_teams(), event_id="e1")
        assert len(merged) == 1
        p = merged[0]
        assert p["goals"] == 0
        assert p["assists"] == 0
        assert p["shots"] == 0
        assert p["saves"] == 0
        assert p["yellow_cards"] == 0
        assert p["red_cards"] == 0

    def test_player_with_stats_gets_values(self):
        lineup = _make_lineup([_player("p1")])
        # Nova estrutura: lista de {playerId, statsKey, numericValue}
        stats = [
            {"playerId": "p1", "statsKey": "goals", "numericValue": 2},
            {"playerId": "p1", "statsKey": "assistsGoal", "numericValue": 1},
            {"playerId": "p1", "statsKey": "shotsTotal", "numericValue": 5},
        ]
        merged = _merge_lineup_stats(lineup, stats, _make_teams(), event_id="e1")
        p = merged[0]
        assert p["goals"] == 2
        assert p["assists"] == 1
        assert p["shots"] == 5

    def test_stats_as_list(self):
        lineup = _make_lineup([_player("p1")])
        stats = [
            {"playerId": "p1", "statsKey": "goals", "numericValue": 3},
        ]
        merged = _merge_lineup_stats(lineup, stats, _make_teams(), event_id="e1")
        assert merged[0]["goals"] == 3

    def test_minutes_from_field(self):
        lineup = _make_lineup([_player("p1")])
        # matchMinutesPlayed via stats
        stats = [{"playerId": "p1", "statsKey": "matchMinutesPlayed", "numericValue": 60}]
        merged = _merge_lineup_stats(lineup, stats, _make_teams(), event_id="e1")
        assert merged[0]["minutes"] == 60

    def test_substitute_default_minutes(self):
        lineup = [
            {
                "group": "Substitutes",
                "home": [_player("p2")],
                "away": [],
            }
        ]
        merged = _merge_lineup_stats(lineup, [], _make_teams(), event_id="e1")
        assert merged[0]["minutes"] == 30


# ---------------------------------------------------------------------------
# Cache behaviour (via get_player_season_stats)
# ---------------------------------------------------------------------------

class TestCacheBehaviour:
    def setup_method(self):
        # Limpar cache antes de cada teste
        _cache.clear()

    def _minimal_player_data(self) -> list[dict]:
        """Retorna dados mínimos para um jogador válido."""
        return [
            {
                "player_id": "p1",
                "player_name": "Jogador Um",
                "team_id": "t1",
                "team_name": "Time A",
                "position": "MID",
                "minutes": 200,
                "is_substitute": False,
                "goals": 1,
                "assists": 0,
                "shots": 3,
                "shots_on_target": 1,
                "fouls_committed": 2,
                "yellow_cards": 0,
                "red_cards": 0,
                "saves": 0,
                "xg": 0.5,
                "rating": 7.0,
            }
        ]

    @patch("app.providers.sportdb_scout.get_match_player_stats")
    @patch("app.providers.sportdb_scout.get_season_results")
    def test_cache_miss_calls_http(self, mock_season, mock_match):
        """Cache miss → get_season_results e get_match_player_stats são chamados."""
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = self._minimal_player_data()

        get_player_season_stats(season="test_miss", min_minutes=0)

        mock_season.assert_called_once()
        mock_match.assert_called_once_with("ev1")

    @patch("app.providers.sportdb_scout.get_match_player_stats")
    @patch("app.providers.sportdb_scout.get_season_results")
    def test_cache_hit_no_http(self, mock_season, mock_match):
        """Cache hit dentro do TTL → nenhuma nova requisição."""
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = self._minimal_player_data()

        get_player_season_stats(season="test_hit", min_minutes=0)
        # Segunda chamada — deve usar cache
        get_player_season_stats(season="test_hit", min_minutes=0)

        # Cada mock deve ter sido chamado apenas uma vez (na primeira chamada)
        mock_season.assert_called_once()
        mock_match.assert_called_once()


# ---------------------------------------------------------------------------
# Derived metrics via get_player_season_stats
# ---------------------------------------------------------------------------

class TestDerivedMetrics:
    def setup_method(self):
        _cache.clear()

    def _run_with_players(self, players: list[dict], season: str = "test_dm") -> list[dict]:
        with patch("app.providers.sportdb_scout.get_season_results") as ms, \
             patch("app.providers.sportdb_scout.get_match_player_stats") as mm:
            ms.return_value = [{"id": "ev1"}]
            mm.return_value = players
            return get_player_season_stats(season=season, min_minutes=0)

    def _base_player(self, **overrides) -> dict:
        base = {
            "player_id": "p1",
            "player_name": "Test",
            "team_id": "t1",
            "team_name": "Team",
            "position": "FWD",
            "minutes": 200,
            "is_substitute": False,
            "goals": 0,
            "assists": 0,
            "shots": 0,
            "shots_on_target": 0,
            "fouls_committed": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "saves": 0,
            "xg": 0.0,
            "rating": 0.0,
        }
        base.update(overrides)
        return base

    def test_conversion_rate_shots_zero(self):
        """conversion_rate com shots=0 → 0.0"""
        result = self._run_with_players([self._base_player(goals=3, shots=0)], season="cr_zero")
        assert len(result) == 1
        assert result[0]["conversion_rate"] == 0.0

    def test_conversion_rate_with_shots(self):
        """conversion_rate = goals / shots quando shots > 0"""
        result = self._run_with_players([self._base_player(goals=2, shots=4)], season="cr_val")
        assert abs(result[0]["conversion_rate"] - 0.5) < 1e-9

    def test_save_rate_denominator_zero(self):
        """save_rate com saves=0 e goals_conceded=0 → 0.0"""
        result = self._run_with_players(
            [self._base_player(position="GKP", saves=0)], season="sr_zero"
        )
        assert len(result) == 1
        assert result[0]["save_rate"] == 0.0

    def test_save_rate_with_values(self):
        """save_rate = saves / (saves + goals_conceded)"""
        player = self._base_player(position="GKP", saves=8)
        # goals_conceded não vem do player diretamente no merge, mas podemos simular
        # adicionando goals_conceded ao dict retornado pelo mock
        player["goals_conceded"] = 2
        result = self._run_with_players([player], season="sr_val")
        assert abs(result[0]["save_rate"] - 0.8) < 1e-9


# ---------------------------------------------------------------------------
# Error handling: partida com erro é ignorada
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def setup_method(self):
        _cache.clear()

    def test_match_error_skipped_others_processed(self):
        """Erro em partida individual → partida ignorada, demais processadas."""
        good_player = {
            "player_id": "p_good",
            "player_name": "Good Player",
            "team_id": "t1",
            "team_name": "Team",
            "position": "MID",
            "minutes": 200,
            "is_substitute": False,
            "goals": 1,
            "assists": 0,
            "shots": 2,
            "shots_on_target": 1,
            "fouls_committed": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "saves": 0,
            "xg": 0.3,
            "rating": 7.0,
        }

        def side_effect(event_id: str):
            if event_id == "ev_bad":
                raise RuntimeError("API error")
            return [good_player]

        with patch("app.providers.sportdb_scout.get_season_results") as ms, \
             patch("app.providers.sportdb_scout.get_match_player_stats") as mm:
            ms.return_value = [{"id": "ev_bad"}, {"id": "ev_good"}]
            mm.side_effect = side_effect

            result = get_player_season_stats(season="err_test", min_minutes=0)

        # O jogador da partida boa deve aparecer
        assert any(p["player_id"] == "p_good" for p in result)


# ---------------------------------------------------------------------------
# Property-Based Tests (hypothesis)
# ---------------------------------------------------------------------------

from hypothesis import given, settings, assume
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Property 1: Cache hit idempotência
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(season=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))))
def test_property1_cache_hit_idempotency(season):
    # Feature: sportdb-scout-migration, Property 1: Cache hit idempotência
    _cache.clear()

    match_player = {
        "player_id": "p1",
        "player_name": "Player",
        "team_id": "t1",
        "team_name": "Team",
        "position": "MID",
        "minutes": 200,
        "is_substitute": False,
        "goals": 1,
        "assists": 0,
        "shots": 2,
        "shots_on_target": 1,
        "fouls_committed": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "saves": 0,
        "xg": 0.0,
        "rating": 7.0,
    }

    with patch("app.providers.sportdb_scout.get_season_results") as mock_season, \
         patch("app.providers.sportdb_scout.get_match_player_stats") as mock_match:
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = [match_player]

        result1 = get_player_season_stats(season=season, min_minutes=0)
        result2 = get_player_season_stats(season=season, min_minutes=0)

    assert result1 == result2
    # Segunda chamada deve usar cache — mocks chamados apenas uma vez
    mock_season.assert_called_once()
    mock_match.assert_called_once()


# ---------------------------------------------------------------------------
# Property 4: Agregação completa de partidas
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(match_ids=st.lists(st.text(min_size=1, max_size=8, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"), min_size=1, max_size=20, unique=True))
def test_property4_full_aggregation(match_ids):
    # Feature: sportdb-scout-migration, Property 4: Agregação completa
    _cache.clear()

    match_player = {
        "player_id": "p1",
        "player_name": "Player",
        "team_id": "t1",
        "team_name": "Team",
        "position": "MID",
        "minutes": 90,
        "is_substitute": False,
        "goals": 0,
        "assists": 0,
        "shots": 0,
        "shots_on_target": 0,
        "fouls_committed": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "saves": 0,
        "xg": 0.0,
        "rating": 0.0,
    }

    season_key = "prop4_" + "_".join(match_ids[:3])

    with patch("app.providers.sportdb_scout.get_season_results") as mock_season, \
         patch("app.providers.sportdb_scout.get_match_player_stats") as mock_match:
        mock_season.return_value = [{"id": mid} for mid in match_ids]
        mock_match.return_value = [match_player]

        get_player_season_stats(season=season_key, min_minutes=0)

    assert mock_match.call_count == len(match_ids)


# ---------------------------------------------------------------------------
# Property 5: Métricas derivadas matematicamente corretas
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    goals=st.integers(min_value=0, max_value=50),
    shots=st.integers(min_value=1, max_value=100),
    saves=st.integers(min_value=0, max_value=50),
    goals_conceded=st.integers(min_value=0, max_value=50),
)
def test_property5_derived_metrics(goals, shots, saves, goals_conceded):
    # Feature: sportdb-scout-migration, Property 5: Métricas derivadas matematicamente corretas
    _cache.clear()

    season_key = f"prop5_{goals}_{shots}_{saves}_{goals_conceded}"

    player = {
        "player_id": "p1",
        "player_name": "Player",
        "team_id": "t1",
        "team_name": "Team",
        "position": "FWD",
        "minutes": 200,
        "is_substitute": False,
        "goals": goals,
        "assists": 0,
        "shots": shots,
        "shots_on_target": 0,
        "fouls_committed": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "saves": saves,
        "goals_conceded": goals_conceded,
        "xg": 0.0,
        "rating": 0.0,
    }

    with patch("app.providers.sportdb_scout.get_season_results") as mock_season, \
         patch("app.providers.sportdb_scout.get_match_player_stats") as mock_match:
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = [player]

        result = get_player_season_stats(season=season_key, min_minutes=0)

    assert len(result) == 1
    p = result[0]

    expected_conversion = goals / shots
    assert abs(p["conversion_rate"] - expected_conversion) < 1e-9

    denom = saves + goals_conceded
    if denom > 0:
        expected_save_rate = saves / denom
        assert abs(p["save_rate"] - expected_save_rate) < 1e-9
    else:
        assert p["save_rate"] == 0.0


# ---------------------------------------------------------------------------
# Property 6: Filtro min_minutes
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(min_minutes=st.integers(min_value=0, max_value=500))
def test_property6_min_minutes_filter(min_minutes):
    # Feature: sportdb-scout-migration, Property 6: Filtro min_minutes
    _cache.clear()

    season_key = f"prop6_{min_minutes}"

    players = [
        {
            "player_id": f"p{i}",
            "player_name": f"Player {i}",
            "team_id": "t1",
            "team_name": "Team",
            "position": "MID",
            "minutes": i * 30,
            "is_substitute": False,
            "goals": 0,
            "assists": 0,
            "shots": 0,
            "shots_on_target": 0,
            "fouls_committed": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "saves": 0,
            "xg": 0.0,
            "rating": 0.0,
        }
        for i in range(1, 20)
    ]

    with patch("app.providers.sportdb_scout.get_season_results") as mock_season, \
         patch("app.providers.sportdb_scout.get_match_player_stats") as mock_match:
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = players

        result = get_player_season_stats(season=season_key, min_minutes=min_minutes)

    for p in result:
        assert p["total_minutes"] >= min_minutes


# ---------------------------------------------------------------------------
# Property 7: Merge lineup+stats — um registro por player_id único do lineup
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    player_ids=st.lists(
        st.text(min_size=1, max_size=6, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"),
        min_size=1,
        max_size=11,
        unique=True,
    )
)
def test_property7_merge_one_record_per_player(player_ids):
    # Feature: sportdb-scout-migration, Property 7: Merge lineup+stats
    lineup = _make_lineup(
        [_player(pid) for pid in player_ids]
    )
    merged = _merge_lineup_stats(lineup, [], _make_teams(), event_id="ev1")

    result_ids = [r["player_id"] for r in merged]
    assert len(result_ids) == len(player_ids)
    assert set(result_ids) == set(player_ids)


# ---------------------------------------------------------------------------
# Property 11: Métricas p90 >= 0
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    goals=st.integers(min_value=0, max_value=30),
    assists=st.integers(min_value=0, max_value=30),
    shots=st.integers(min_value=0, max_value=50),
    shots_on_target=st.integers(min_value=0, max_value=50),
    fouls=st.integers(min_value=0, max_value=20),
    yellow_cards=st.integers(min_value=0, max_value=5),
    red_cards=st.integers(min_value=0, max_value=2),
    saves=st.integers(min_value=0, max_value=20),
    goals_conceded=st.integers(min_value=0, max_value=20),
    minutes=st.integers(min_value=1, max_value=3000),
)
def test_property11_p90_metrics_non_negative(goals, assists, shots, shots_on_target, fouls, yellow_cards, red_cards, saves, goals_conceded, minutes):
    # Feature: sportdb-scout-migration, Property 11: Métricas p90 >= 0
    _cache.clear()

    season_key = f"prop11_{goals}_{assists}_{shots}_{minutes}"

    player = {
        "player_id": "p1",
        "player_name": "Player",
        "team_id": "t1",
        "team_name": "Team",
        "position": "MID",
        "minutes": minutes,
        "is_substitute": False,
        "goals": goals,
        "assists": assists,
        "shots": shots,
        "shots_on_target": shots_on_target,
        "fouls_committed": fouls,
        "yellow_cards": yellow_cards,
        "red_cards": red_cards,
        "saves": saves,
        "goals_conceded": goals_conceded,
        "xg": 0.0,
        "rating": 0.0,
    }

    with patch("app.providers.sportdb_scout.get_season_results") as mock_season, \
         patch("app.providers.sportdb_scout.get_match_player_stats") as mock_match:
        mock_season.return_value = [{"id": "ev1"}]
        mock_match.return_value = [player]

        result = get_player_season_stats(season=season_key, min_minutes=0)

    p90_fields = [
        "goals_p90", "assists_p90", "shots_p90", "shots_on_target_p90",
        "fouls_p90", "yellow_cards_p90", "red_cards_p90",
        "saves_p90", "goals_conceded_p90",
    ]

    for p in result:
        for field in p90_fields:
            val = p[field]
            assert isinstance(val, float), f"{field} deve ser float, got {type(val)}"
            assert val >= 0.0, f"{field} deve ser >= 0.0, got {val}"
