from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class TeamMetricAverages(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    possession: float | None = None
    shots: float | None = None
    shots_on_target: float | None = None
    passes: float | None = None
    pass_accuracy: float | None = None
    corners: float | None = None
    fouls: float | None = None
    yellow_cards: float | None = None
    red_cards: float | None = None
    xg: float | None = None
    goals_for: float | None = None
    goals_against: float | None = None


class TeamTrend(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    possession: float | None = None
    shots: float | None = None
    shots_on_target: float | None = None
    passes: float | None = None
    pass_accuracy: float | None = None
    corners: float | None = None
    fouls: float | None = None
    yellow_cards: float | None = None
    red_cards: float | None = None
    xg: float | None = None
    goals_for: float | None = None
    goals_against: float | None = None


class TeamRadar(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    team_id: int
    competition_id: int
    window: int
    min_matches: int
    min_teams: int
    eligible_teams: int
    metrics: dict[str, float | None]
    note: str | None = None


class TeamTimeSeriesPoint(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    match_id: int
    round_number: int
    match_date_time: datetime
    is_home: bool
    goals_for: int | None
    goals_against: int | None
    possession: float | None
    xg: float | None
    shots: int | None
    passes: int | None


class TeamAnalyticsSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    team_id: int
    competition_id: int
    window: int
    averages: TeamMetricAverages
    trend: TeamTrend | None
    last_matches: list[int]


class TopScorerItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    player_id: int
    name: str
    goals: int
    assists: int
    matches_played: int


class TopScorersResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    team_id: str
    top_scorers: list[TopScorerItem]


class TopStatItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    player_id: int
    name: str
    value: float
    matches_played: int
