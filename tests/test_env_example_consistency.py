from pathlib import Path

ENV_EXAMPLE_PATH = Path(__file__).resolve().parent.parent / ".env.example"


def _keys_in_env_example() -> set[str]:
    keys = set()
    for line in ENV_EXAMPLE_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0])
    return keys


def test_env_example_documents_required_safety_settings():
    keys = _keys_in_env_example()
    required = {
        "SOAK_MODE",
        "DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS",
        "DHAN_MARKETQUOTE_MIN_INTERVAL_SECONDS",
        "ORCHESTRATOR_LOCK_TTL_SECONDS",
        "GAP_ALERT_COOLDOWN_SECONDS",
    }
    missing = required - keys
    assert not missing, f".env.example is missing required settings: {missing}"


def test_env_example_has_no_fake_secrets():
    text = ENV_EXAMPLE_PATH.read_text()
    for line in text.splitlines():
        if line.strip().startswith(("DHAN_ACCESS_TOKEN", "DHAN_CLIENT_ID", "TELEGRAM_BOT_TOKEN", "MARKETAUX_API_KEY")):
            value = line.split("=", 1)[1].strip() if "=" in line else ""
            assert value == "", f"{line!r} should be blank in .env.example, not a real-looking value"


def test_env_and_env_example_declare_the_same_variable_names():
    """.env (real, gitignored) and .env.example (committed) must stay in sync so a
    fresh checkout's example file actually reflects what config/settings.py reads."""
    env_path = ENV_EXAMPLE_PATH.parent / ".env"
    if not env_path.exists():
        return  # nothing to compare against in an environment without a real .env

    def _keys(path):
        keys = set()
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            keys.add(line.split("=", 1)[0])
        return keys

    assert _keys(env_path) == _keys_in_env_example()
