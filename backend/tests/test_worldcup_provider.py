"""
Property-based tests for World Cup 2026 backend.
Tests use Hypothesis to verify correctness properties of the worldcup provider and router.
"""

import copy
import datetime
from unittest.mock import patch, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st

from app.main import app
from app.providers.sportdb_worldcup import (
    _cache,
    _cache_lock,
    _cached_get,
    _smart_ttl,
    get_copa_standings,
    get_copa_fixtures,
    get_copa_results,
    get_match_stats,
    get_match_lineups,
    get_team_squad,
)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TTL Configuration Matrix (from design document)
# ---------------------------------------------------------------------------
# | Data Type   | Match Window (12–23 BRT) | Off-Hours (0–11 BRT) |
# |-------------|--------------------------|----------------------|
# | standings   | 60s                      | 3600s                |
# | results     | 60s                      | 300s                 |
# | fixtures    | 3600s                    | 3600s                |
# | squad       | 86400s                   | 86400s               |
# | match_stats | 86400s                   | 86400s               |
# | lineups     | 86400s                   | 86400s               |

_TTL_MATRIX = {
    "standings":   {"match_window": 60,    "off_hours": 3600},
    "results":     {"match_window": 60,    "off_hours": 300},
    "fixtures":    {"match_window": 3600,  "off_hours": 3600},
    "squad":       {"match_window": 86400, "off_hours": 86400},
    "match_stats": {"match_window": 86400, "off_hours": 86400},
    "lineups":     {"match_window": 86400, "off_hours": 86400},
}


# ---------------------------------------------------------------------------
# Property 1: TTL correctness based on data type and match window
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    data_type=st.sampled_from(["standings", "results", "fixtures", "squad", "match_stats", "lineups"]),
    hour=st.integers(min_value=0, max_value=23),
)
def test_property1_ttl_correctness(data_type: str, hour: int):
    """
    Feature: worldcup-backend, Property 1: TTL correctness based on data type and match window

    For any (data_type, hour) pair, _smart_ttl returns the correct TTL value
    as defined in the TTL Configuration Matrix.

    **Validates: Requirements 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8**
    """
    # Create a fixed datetime in BRT (UTC-3) with the given hour
    brt = datetime.timezone(datetime.timedelta(hours=-3))
    fake_now = datetime.datetime(2026, 6, 15, hour, 30, 0, tzinfo=brt)

    with patch("app.providers.sportdb_worldcup.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = fake_now
        mock_datetime.timezone = datetime.timezone
        mock_datetime.timedelta = datetime.timedelta

        result = _smart_ttl(data_type)

    # Determine expected TTL based on match window
    is_match_window = 12 <= hour <= 23
    if is_match_window:
        expected = _TTL_MATRIX[data_type]["match_window"]
    else:
        expected = _TTL_MATRIX[data_type]["off_hours"]

    assert result == expected, (
        f"_smart_ttl('{data_type}') at hour={hour} (match_window={is_match_window}) "
        f"returned {result}, expected {expected}"
    )


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Strategy for a player dict that may or may not have a "countryName" field
_player_base_fields = st.fixed_dictionaries({
    "id": st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"),
    "slug": st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz-"),
    "firstName": st.text(min_size=1, max_size=20),
    "lastName": st.text(min_size=1, max_size=20),
    "jerseyNumber": st.one_of(st.none(), st.integers(min_value=1, max_value=99).map(str)),
    "position": st.sampled_from(["Goalkeepers", "Defenders", "Midfielders", "Forwards"]),
    "link": st.text(min_size=0, max_size=50),
})

# Optional countryName field with various string values (including empty)
_optional_country_name = st.one_of(
    st.just({}),  # no countryName key
    st.fixed_dictionaries({"countryName": st.text(min_size=0, max_size=50)}),
)


@st.composite
def player_dict_strategy(draw):
    """Generate a player dict that may or may not have a countryName field."""
    base = draw(_player_base_fields)
    extra = draw(_optional_country_name)
    return {**base, **extra}


# Strategy for a list of players (1 to 15 players)
players_list_strategy = st.lists(player_dict_strategy(), min_size=1, max_size=15)


# ---------------------------------------------------------------------------
# Property 6: Squad normalization transforms countryName to club
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(players=players_list_strategy)
def test_property6_squad_normalization_countryname_to_club(players):
    """
    Property 6: Squad normalization transforms countryName to club.

    For any list of player dicts returned by the provider, the squad endpoint
    SHALL produce output where each player has a `club` field equal to the
    original `countryName` value (or None if `countryName` was absent), and
    no player has a `countryName` field in the output.

    **Validates: Requirements 10.2**
    """
    # Build a mock provider response with the generated players
    mock_squad_response = {
        "id": "test123",
        "slug": "test-team",
        "teamName": "Test Team",
        "teamLogo": "https://example.com/logo.png",
        "squad": [
            {
                "tournamentId": "tourney1",
                "players": players,
            }
        ],
    }

    with patch("app.routers.worldcup.get_team_squad", return_value=mock_squad_response):
        response = client.get("/worldcup/team/test-team/test123/squad")

    assert response.status_code == 200
    data = response.json()

    # The response should have a "squad" key with normalized entries
    assert "squad" in data
    squad_entries = data["squad"]
    assert len(squad_entries) == 1

    normalized_players = squad_entries[0]["players"]
    assert len(normalized_players) == len(players)

    for i, norm_player in enumerate(normalized_players):
        original = players[i]

        # Each player MUST have a "club" field
        assert "club" in norm_player, f"Player {i} missing 'club' field"

        # The "club" value must equal the original "countryName" or None if absent
        expected_club = original.get("countryName")  # None if key absent
        assert norm_player["club"] == expected_club, (
            f"Player {i}: expected club={expected_club!r}, got {norm_player['club']!r}"
        )

        # No player should have a "countryName" field in the output
        assert "countryName" not in norm_player, (
            f"Player {i} still has 'countryName' field in normalized output"
        )


# ---------------------------------------------------------------------------
# Property 7: Valid page parameter passthrough
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(page=st.integers(min_value=1, max_value=1000))
def test_property7_page_parameter_passthrough_fixtures(page):
    """
    Property 7: Valid page parameter passthrough (fixtures endpoint).

    For any integer p >= 1, when passed as the page query parameter to
    /worldcup/fixtures, the router SHALL invoke the provider function with page=p.

    **Validates: Requirements 6.2, 7.2**
    """
    with patch("app.routers.worldcup.get_copa_fixtures") as mock_fixtures:
        mock_fixtures.return_value = [{"match": "data"}]

        response = client.get(f"/worldcup/fixtures?page={page}")

        assert response.status_code == 200
        mock_fixtures.assert_called_once_with(page)


@settings(max_examples=100)
@given(page=st.integers(min_value=1, max_value=1000))
def test_property7_page_parameter_passthrough_results(page):
    """
    Property 7: Valid page parameter passthrough (results endpoint).

    For any integer p >= 1, when passed as the page query parameter to
    /worldcup/results, the router SHALL invoke the provider function with page=p.

    **Validates: Requirements 6.2, 7.2**
    """
    with patch("app.routers.worldcup.get_copa_results") as mock_results:
        mock_results.return_value = [{"match": "result_data"}]

        response = client.get(f"/worldcup/results?page={page}")

        assert response.status_code == 200
        mock_results.assert_called_once_with(page)


# ---------------------------------------------------------------------------
# Property 5: All endpoints return HTTP 502 on provider exception
# ---------------------------------------------------------------------------

# Strategy for random exception types that a provider might raise
_exception_types = st.sampled_from([RuntimeError, ConnectionError, TimeoutError, ValueError, IOError])

# All 6 endpoints with their corresponding provider function mock paths
_ENDPOINTS_AND_PROVIDERS = [
    ("/worldcup/standings", "app.routers.worldcup.get_copa_standings"),
    ("/worldcup/fixtures", "app.routers.worldcup.get_copa_fixtures"),
    ("/worldcup/results", "app.routers.worldcup.get_copa_results"),
    ("/worldcup/match/test123/stats", "app.routers.worldcup.get_match_stats"),
    ("/worldcup/match/test123/lineups", "app.routers.worldcup.get_match_lineups"),
    ("/worldcup/team/brazil/I9l9aqLq/squad", "app.routers.worldcup.get_team_squad"),
]


@settings(max_examples=100)
@given(
    exc_type=_exception_types,
    endpoint_idx=st.integers(min_value=0, max_value=5),
)
def test_property5_all_endpoints_return_502_on_provider_exception(exc_type, endpoint_idx):
    """
    Property 5: All endpoints return HTTP 502 on provider exception.

    For any of the 6 World Cup endpoints, when the underlying provider function
    raises any exception, the router SHALL return an HTTP 502 response with a
    JSON body containing a `detail` string field.

    **Validates: Requirements 5.4, 8.3, 9.3, 10.4, 11.1, 11.4**
    """
    endpoint_path, provider_mock_path = _ENDPOINTS_AND_PROVIDERS[endpoint_idx]
    exc = exc_type("simulated provider failure")

    with patch(provider_mock_path, side_effect=exc):
        response = client.get(endpoint_path)

    assert response.status_code == 502, (
        f"Expected 502 for {endpoint_path} when provider raises {exc_type.__name__}, "
        f"got {response.status_code}"
    )

    body = response.json()
    assert "detail" in body, (
        f"Expected 'detail' field in 502 response body for {endpoint_path}, "
        f"got keys: {list(body.keys())}"
    )
    assert isinstance(body["detail"], str), (
        f"Expected 'detail' to be a string for {endpoint_path}, "
        f"got {type(body['detail']).__name__}"
    )


# ---------------------------------------------------------------------------
# Property 2: Cache hit returns data without API call
# ---------------------------------------------------------------------------

# Strategy for generating random cached data payloads
_cached_data_strategy = st.one_of(
    st.lists(st.fixed_dictionaries({"id": st.text(min_size=1, max_size=10), "value": st.integers()}), min_size=1, max_size=5),
    st.fixed_dictionaries({"result": st.text(min_size=1, max_size=20)}),
)

# Strategy for data types and their corresponding provider functions + cache keys
_data_type_configs = st.sampled_from([
    ("standings", "copa_standings", "get_copa_standings", (), {}),
    ("fixtures", "copa_fixtures_1", "get_copa_fixtures", (1,), {}),
    ("results", "copa_results_1", "get_copa_results", (1,), {}),
    ("match_stats", "copa_match_stats_ev123", "get_match_stats", ("ev123",), {}),
    ("lineups", "copa_lineups_ev123", "get_match_lineups", ("ev123",), {}),
    ("squad", "copa_squad_tid1", "get_team_squad", ("team-slug", "tid1"), {}),
])

# Strategy for cache age as a fraction of TTL (0 to 0.99 ensures age < TTL)
_cache_age_fraction = st.floats(min_value=0.0, max_value=0.99, allow_nan=False, allow_infinity=False)


@settings(max_examples=200)
@given(
    config=_data_type_configs,
    cached_data=_cached_data_strategy,
    age_fraction=_cache_age_fraction,
)
def test_property2_cache_hit_returns_data_without_api_call(config, cached_data, age_fraction):
    """
    Property 2: Cache hit returns data without API call.

    For any cache key with a stored entry whose age is less than the applicable TTL,
    calling the corresponding provider function SHALL return the cached data without
    making an HTTP request to the SportDB API.

    **Validates: Requirements 2.3**
    """
    data_type, cache_key, func_name, args, kwargs = config

    # Map function names to actual functions
    func_map = {
        "get_copa_standings": get_copa_standings,
        "get_copa_fixtures": get_copa_fixtures,
        "get_copa_results": get_copa_results,
        "get_match_stats": get_match_stats,
        "get_match_lineups": get_match_lineups,
        "get_team_squad": get_team_squad,
    }
    provider_fn = func_map[func_name]

    # Calculate a cache timestamp that is within TTL
    # We need to get the TTL for this data type at the current time
    ttl = _smart_ttl(data_type)
    age_seconds = age_fraction * ttl  # age < TTL guaranteed since fraction < 1.0
    cache_ts = datetime.datetime.now() - datetime.timedelta(seconds=age_seconds)

    # Pre-populate cache with a valid entry
    with _cache_lock:
        _cache[cache_key] = {"data": cached_data, "ts": cache_ts}

    try:
        with patch("app.providers.sportdb_worldcup.httpx.get") as mock_httpx_get:
            result = provider_fn(*args, **kwargs)

            # Verify no HTTP call was made (cache hit)
            mock_httpx_get.assert_not_called()

            # Verify the returned data matches the cached data
            assert result == cached_data, (
                f"Cache hit for {func_name} returned {result!r}, expected {cached_data!r}"
            )
    finally:
        # Clean up cache to avoid test pollution
        with _cache_lock:
            _cache.pop(cache_key, None)


# ---------------------------------------------------------------------------
# Property 3: Failed fetch preserves cache state and propagates exception
# ---------------------------------------------------------------------------

# Strategy for generating random JSON-serializable cache data
_random_cache_value = st.one_of(
    st.text(min_size=1, max_size=30),
    st.integers(),
    st.lists(st.integers(), min_size=0, max_size=5),
    st.fixed_dictionaries({"key": st.text(min_size=1, max_size=10), "val": st.integers()}),
)

# Strategy for choosing which exception type to simulate
_failure_exception_type = st.sampled_from(["timeout", "http_status_error"])


@settings(max_examples=100)
@given(
    initial_cache_data=_random_cache_value,
    exception_type=_failure_exception_type,
)
def test_property3_failed_fetch_preserves_cache_state(initial_cache_data, exception_type):
    """
    Property 3: Failed fetch preserves cache state and propagates exception.

    For any provider function call where the SportDB API returns a non-2xx status
    code or times out, the provider SHALL raise an exception to the caller AND the
    cache dictionary SHALL remain unchanged (no new entry added, no existing entry
    modified).

    **Validates: Requirements 1.6, 2.5, 11.2, 11.3**
    """
    cache_key = "copa_standings"
    # Create an expired cache entry (age > any possible TTL, use 100000s)
    expired_ts = datetime.datetime.now() - datetime.timedelta(seconds=100000)

    # Pre-populate _cache with an expired entry
    with _cache_lock:
        _cache[cache_key] = {"data": initial_cache_data, "ts": expired_ts}

    # Take a deep copy of the cache state before the failed fetch
    with _cache_lock:
        cache_snapshot_before = copy.deepcopy(dict(_cache))

    # Build the exception to simulate
    if exception_type == "timeout":
        exc = httpx.TimeoutException("timeout")
    else:
        # HTTPStatusError requires a request and response
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        exc = httpx.HTTPStatusError("error", request=mock_request, response=mock_response)

    try:
        with patch("app.providers.sportdb_worldcup.httpx.get", side_effect=exc):
            # Call the provider function — it should raise
            with pytest.raises((httpx.TimeoutException, httpx.HTTPStatusError)):
                get_copa_standings()

        # Verify cache is unchanged after the failed fetch
        with _cache_lock:
            cache_after = copy.deepcopy(dict(_cache))

        assert cache_after == cache_snapshot_before, (
            f"Cache was modified after failed fetch! "
            f"Before: {cache_snapshot_before}, After: {cache_after}"
        )
    finally:
        # Clean up cache to avoid test pollution
        with _cache_lock:
            _cache.pop(cache_key, None)
