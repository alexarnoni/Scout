"""Tests for the market_value_cache module.

Property-based tests (Hypothesis) + edge-case unit tests.
"""

import json
import logging
import os
from datetime import datetime, timedelta

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from app.providers import market_value_cache as mvc


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """Reset module-level state and point CACHE_FILE_PATH to a temp dir."""
    cache_file = str(tmp_path / "market_values.json")
    monkeypatch.setattr(mvc, "CACHE_FILE_PATH", cache_file)
    mvc._cache.clear()
    yield
    mvc._cache.clear()


# Hypothesis strategies
_player_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "Zs")),
    min_size=1,
    max_size=30,
)

_market_value_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S")),
    min_size=1,
    max_size=20,
)


def _iso_now() -> str:
    return datetime.now().isoformat()


def _iso_old(days: int = 8) -> str:
    return (datetime.now() - timedelta(days=days)).isoformat()


# Strategy: valid cache entry (value + recent fetched_at)
_cache_entry_st = st.fixed_dictionaries({
    "value": _market_value_st,
    "fetched_at": st.builds(
        lambda d: (datetime(2025, 1, 1) + timedelta(days=d)).isoformat(),
        st.integers(min_value=0, max_value=3650),
    ),
})

# Strategy: full cache dict
_cache_dict_st = st.dictionaries(
    keys=_player_name_st,
    values=_cache_entry_st,
    min_size=0,
    max_size=15,
)


# ---------------------------------------------------------------------------
# 2.1 [PBT] JSON serialization round-trip
# ---------------------------------------------------------------------------

class TestJsonSerializationRoundTrip:
    """Property 1: JSON round-trip.

    **Validates: Requirements 2.1**
    """

    @given(cache_dict=_cache_dict_st)
    @settings(max_examples=100)
    def test_json_roundtrip(self, cache_dict):
        """json.loads(json.dumps(cache_dict)) == cache_dict for any valid cache dict."""
        serialized = json.dumps(cache_dict)
        deserialized = json.loads(serialized)
        assert deserialized == cache_dict


# ---------------------------------------------------------------------------
# 2.2 [PBT] Stats invariant
# ---------------------------------------------------------------------------

class TestStatsInvariant:
    """Property 2: Stats invariant total == valid + expired.

    **Validates: Requirements 1.12**
    """

    @given(
        valid_entries=st.dictionaries(
            keys=st.text(
                alphabet=st.characters(whitelist_categories=("L",)),
                min_size=1,
                max_size=10,
            ).map(lambda s: "v_" + s),
            values=_market_value_st,
            min_size=0,
            max_size=10,
        ),
        expired_entries=st.dictionaries(
            keys=st.text(
                alphabet=st.characters(whitelist_categories=("L",)),
                min_size=1,
                max_size=10,
            ).map(lambda s: "e_" + s),
            values=_market_value_st,
            min_size=0,
            max_size=10,
        ),
    )
    @settings(max_examples=100)
    def test_stats_total_equals_valid_plus_expired(self, valid_entries, expired_entries):
        """stats['total'] == stats['valid'] + stats['expired'] for any cache state."""
        mvc._cache.clear()

        # Insert valid entries (recent timestamp)
        for name, value in valid_entries.items():
            mvc._cache[name] = {"value": value, "fetched_at": _iso_now()}

        # Insert expired entries (old timestamp)
        for name, value in expired_entries.items():
            mvc._cache[name] = {"value": value, "fetched_at": _iso_old(days=8)}

        stats = mvc.get_cache_stats()
        assert stats["total"] == stats["valid"] + stats["expired"]
        assert stats["total"] == len(mvc._cache)


# ---------------------------------------------------------------------------
# 2.3 [PBT] Set/get round-trip
# ---------------------------------------------------------------------------

class TestSetGetRoundTrip:
    """Property 3: set/get round-trip.

    **Validates: Requirements 1.5, 1.6**
    """

    @given(player_name=_player_name_st, market_value=_market_value_st)
    @settings(max_examples=100)
    def test_set_then_get_returns_same_value(self, player_name, market_value):
        """After set_cached_market_value(name, value), get returns value."""
        mvc._cache.clear()
        mvc.set_cached_market_value(player_name, market_value)
        result = mvc.get_cached_market_value(player_name)
        assert result == market_value


# ---------------------------------------------------------------------------
# 2.4 [PBT] Expiration
# ---------------------------------------------------------------------------

class TestExpiration:
    """Property 4: Expired entries return None.

    **Validates: Requirements 1.7**
    """

    @given(
        player_name=_player_name_st,
        market_value=_market_value_st,
        extra_days=st.integers(min_value=1, max_value=365),
    )
    @settings(max_examples=100)
    def test_expired_entries_return_none(self, player_name, market_value, extra_days):
        """Entries with fetched_at > 7 days ago return None."""
        mvc._cache.clear()
        old_ts = (datetime.now() - timedelta(days=7, seconds=extra_days * 86400)).isoformat()
        mvc._cache[player_name] = {"value": market_value, "fetched_at": old_ts}
        result = mvc.get_cached_market_value(player_name)
        assert result is None


# ---------------------------------------------------------------------------
# 2.5 [PBT] Disk persistence round-trip
# ---------------------------------------------------------------------------

class TestDiskPersistenceRoundTrip:
    """Property 5: Disk persistence round-trip.

    **Validates: Requirements 1.1, 1.2, 1.5**
    """

    @given(player_name=_player_name_st, market_value=_market_value_st)
    @settings(max_examples=100)
    def test_set_init_get_roundtrip(self, player_name, market_value):
        """set → init_cache → get returns same value."""
        mvc._cache.clear()
        mvc.set_cached_market_value(player_name, market_value)
        # Reinitialize from disk
        mvc._cache.clear()
        mvc.init_cache()
        result = mvc.get_cached_market_value(player_name)
        assert result == market_value


# ---------------------------------------------------------------------------
# 2.6 Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge-case unit tests for error handling paths."""

    def test_init_cache_nonexistent_file(self):
        """init_cache with non-existent file → empty cache, no error."""
        mvc._cache.clear()
        # CACHE_FILE_PATH already points to tmp_path which has no file
        mvc.init_cache()
        assert mvc._cache == {}

    def test_init_cache_invalid_json(self, tmp_path, monkeypatch, caplog):
        """init_cache with invalid JSON → empty cache, warning logged."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{not valid json!!!", encoding="utf-8")
        monkeypatch.setattr(mvc, "CACHE_FILE_PATH", str(bad_file))
        mvc._cache.clear()

        with caplog.at_level(logging.WARNING):
            mvc.init_cache()

        assert mvc._cache == {}
        assert any("invalid JSON" in r.message for r in caplog.records)

    def test_save_to_disk_io_error(self, tmp_path, monkeypatch, caplog):
        """_save_to_disk with I/O error → error logged, no exception."""
        # Point to a path that can't be written (directory as file)
        read_only_dir = tmp_path / "readonly"
        read_only_dir.mkdir()
        bad_path = str(read_only_dir / "subdir" / "cache.json")
        # Make the parent read-only so makedirs succeeds but open fails
        monkeypatch.setattr(mvc, "CACHE_FILE_PATH", str(read_only_dir))

        mvc._cache["test"] = {"value": "€1M", "fetched_at": _iso_now()}

        with caplog.at_level(logging.ERROR):
            mvc._save_to_disk()  # Should not raise

        assert any("Failed to write" in r.message or "cache file" in r.message.lower() for r in caplog.records)

    def test_init_cache_io_error_reading(self, tmp_path, monkeypatch, caplog):
        """init_cache with I/O error reading → empty cache, warning logged."""
        # Create a directory where a file is expected → OSError on open()
        dir_as_file = tmp_path / "fake_file.json"
        dir_as_file.mkdir()
        monkeypatch.setattr(mvc, "CACHE_FILE_PATH", str(dir_as_file))
        mvc._cache.clear()

        with caplog.at_level(logging.WARNING):
            mvc.init_cache()

        assert mvc._cache == {}
        assert any("I/O error" in r.message or "starting with empty cache" in r.message for r in caplog.records)
