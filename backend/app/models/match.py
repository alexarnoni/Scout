from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, SourceIdsMixin, TimestampMixin


class Match(Base, TimestampMixin, SourceIdsMixin):
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True)
    competition_id: Mapped[int] = mapped_column(
        ForeignKey("competitions.id"), nullable=False
    )
    round_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    match_date_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    external_source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    external_ids: Mapped[dict[str, str] | None] = mapped_column(JSON, nullable=True)
    sportdb_event_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    home_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), nullable=False
    )
    away_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    score_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score_away: Mapped[int | None] = mapped_column(Integer, nullable=True)

    competition: Mapped["Competition"] = relationship(back_populates="matches")
    home_team: Mapped["Team"] = relationship(
        back_populates="home_matches", foreign_keys=[home_team_id]
    )
    away_team: Mapped["Team"] = relationship(
        back_populates="away_matches", foreign_keys=[away_team_id]
    )
    team_stats: Mapped[list["TeamMatchStats"]] = relationship(
        back_populates="match", cascade="all, delete-orphan"
    )
    player_stats: Mapped[list["PlayerMatchStats"]] = relationship(
        back_populates="match", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_matches_competition_id", "competition_id"),
        Index("ix_matches_match_date_time", "match_date_time"),
        UniqueConstraint(
            "external_source", "external_id", name="uq_matches_external_source_id"
        ),
    )
