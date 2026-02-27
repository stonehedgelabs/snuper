"""Configuration management module for loading and accessing application settings.

This module uses shared utilities from arb-py/utils for config management.
"""

from __future__ import annotations

from pathlib import Path
import sys

# Import shared config utilities
# pylint: disable=wrong-import-position,unused-import
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "utils"))
from utils import (
    ServerConfig,
    CacheConfig,
    RdsConfig,
    SeasonsConfig,
    SeasonWindow,
    ApiConfig,
    RedditApiConfig,
    AppConfig,
    load_config as _load_config,
)

# pylint: enable=wrong-import-position,unused-import

# Re-export these for other snuper modules
__all__ = [
    "ServerConfig",
    "CacheConfig",
    "RdsConfig",
    "SeasonsConfig",
    "SeasonWindow",
    "ApiConfig",
    "RedditApiConfig",
    "AppConfig",
    "load_config",
    "get_config",
]

# Global configuration instance.
_CONFIG: AppConfig | None = None


def load_config(path: str | Path) -> AppConfig:
    """Load configuration from a TOML file and store it globally.

    Args:
        path: File path to the TOML configuration file.

    Returns:
        The loaded AppConfig instance.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
    """
    config = _load_config(path)
    global _CONFIG  # pylint: disable=global-statement
    _CONFIG = config
    return config


def get_config() -> AppConfig:
    """Retrieve the globally loaded configuration.

    Returns:
        The current AppConfig instance.

    Raises:
        RuntimeError: If configuration has not been loaded yet.
    """
    if _CONFIG is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Configuration has not been loaded")
    return _CONFIG
