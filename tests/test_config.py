"""Test configuration for snuper."""

from pathlib import Path


def test_config_exists():
    """Test that the config.toml file exists at the expected location."""
    config_path = Path(__file__).parent.parent.parent.parent / "arb-rs" / "config.toml"
    assert config_path.exists(), f"Config file not found at {config_path}"
