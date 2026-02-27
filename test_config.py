"""Test configuration loading for snuper."""

from pathlib import Path

from snuper.config import AppConfig


def test_config_parses():
    """Test that the configuration can be parsed from config.toml without errors."""
    config_path = Path(__file__).parent.parent.parent / "arb-rs" / "config.toml"

    # Check that config file exists
    assert config_path.exists(), f"Config file not found at {config_path}"

    # Load and parse the config - this should not raise any errors
    import tomllib

    with config_path.open("rb") as f:
        data = tomllib.load(f)

    # Parse the config - this will fail if the structure is wrong
    config = AppConfig.from_dict(data)

    # Basic sanity check
    assert config is not None
