from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib


@dataclass(frozen=True, slots=True)
class ServerConfig:
    host: str
    port: int
    cors_origins: str
    client_url: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ServerConfig":
        return cls(
            host=str(data["host"]),
            port=int(data["port"]),
            cors_origins=str(data["cors_origins"]),
            client_url=str(data["client_url"]),
        )


@dataclass(frozen=True, slots=True)
class CacheTTLConfig:
    team_profiles: int
    schedule: int
    postseason_schedule: int
    scores: int
    play_by_play: int
    box_scores: int
    stadiums: int
    standings: int
    twitter_search: int
    reddit_thread: int
    reddit_thread_comments: int
    odds: int
    live_odds: int
    live_odds_games_ids: int
    user_auth: int

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CacheTTLConfig":
        return cls(
            team_profiles=int(data["team_profiles"]),
            schedule=int(data["schedule"]),
            postseason_schedule=int(data["postseason_schedule"]),
            scores=int(data["scores"]),
            play_by_play=int(data["play_by_play"]),
            box_scores=int(data["box_scores"]),
            stadiums=int(data["stadiums"]),
            standings=int(data["standings"]),
            twitter_search=int(data["twitter_search"]),
            reddit_thread=int(data["reddit_thread"]),
            reddit_thread_comments=int(data["reddit_thread_comments"]),
            odds=int(data["odds"]),
            live_odds=int(data["live_odds"]),
            live_odds_games_ids=int(data["live_odds_games_ids"]),
            user_auth=int(data["user_auth"]),
        )


@dataclass(frozen=True, slots=True)
class CacheConfig:
    enabled: bool
    mode: str
    redis_url: str
    default_ttl: int
    ttl: CacheTTLConfig

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CacheConfig":
        ttl_config = CacheTTLConfig.from_dict(data["ttl"])
        return cls(
            enabled=bool(data["enabled"]),
            mode=str(data["mode"]),
            redis_url=str(data["redis_url"]),
            default_ttl=int(data["default_ttl"]),
            ttl=ttl_config,
        )


@dataclass(frozen=True, slots=True)
class RdsConfig:
    enabled: bool
    postgres_uri: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RdsConfig":
        return cls(
            enabled=bool(data["enabled"]),
            postgres_uri=str(data["postgres_uri"]),
        )


@dataclass(frozen=True, slots=True)
class SeasonWindow:
    regular: str
    postseason: str
    postseason_start: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SeasonWindow":
        return cls(
            regular=str(data["regular"]),
            postseason=str(data["postseason"]),
            postseason_start=str(data["postseason_start"]),
        )


@dataclass(frozen=True, slots=True)
class SeasonsConfig:
    current_seasons: dict[str, SeasonWindow]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SeasonsConfig":
        current = {key.lower(): SeasonWindow.from_dict(value) for key, value in data.get("current_seasons", {}).items()}
        return cls(current_seasons=current)

    def get_season(self, league: str) -> SeasonWindow:
        try:
            return self.current_seasons[league.lower()]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise KeyError(f"Unknown league in seasons config: {league}") from exc


@dataclass(frozen=True, slots=True)
class RedditApiConfig:
    default_comment_limit: int
    max_comment_limit: int
    default_sort: str
    user_agent: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RedditApiConfig":
        return cls(
            default_comment_limit=int(data["default_comment_limit"]),
            max_comment_limit=int(data["max_comment_limit"]),
            default_sort=str(data["default_sort"]),
            user_agent=str(data["user_agent"]),
        )


@dataclass(frozen=True, slots=True)
class ApiConfig:
    sportsdata_base_url: str
    rolling_insights_base_url: str
    twitter_base_url: str
    request_timeout: float
    reddit_api: RedditApiConfig

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ApiConfig":
        reddit_api_data = data.get("reddit_api")
        reddit_api = RedditApiConfig.from_dict(reddit_api_data) if reddit_api_data else None
        if reddit_api is None:
            raise ValueError("reddit_api configuration is required")
        return cls(
            sportsdata_base_url=str(data["sportsdata_base_url"]),
            rolling_insights_base_url=str(data["rolling_insights_base_url"]),
            twitter_base_url=str(data["twitter_base_url"]),
            request_timeout=float(data["request_timeout"]),
            reddit_api=reddit_api,
        )


@dataclass(frozen=True, slots=True)
class AppConfig:
    server: ServerConfig
    cache: CacheConfig
    rds: RdsConfig
    seasons: SeasonsConfig
    api: ApiConfig

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AppConfig":
        return cls(
            server=ServerConfig.from_dict(data["server"]),
            cache=CacheConfig.from_dict(data["cache"]),
            rds=RdsConfig.from_dict(data["rds"]),
            seasons=SeasonsConfig.from_dict(data.get("seasons", {})),
            api=ApiConfig.from_dict(data["api"]),
        )


_CONFIG: AppConfig | None = None


def load_config(path: str | Path) -> AppConfig:
    file_path = Path(path).expanduser()
    if not file_path.is_file():  # pragma: no cover - defensive guard
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    with file_path.open("rb") as fh:
        data = tomllib.load(fh)
    config = AppConfig.from_dict(data)
    global _CONFIG  # pylint: disable=global-statement
    _CONFIG = config
    return config


def get_config() -> AppConfig:
    if _CONFIG is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Configuration has not been loaded")
    return _CONFIG
