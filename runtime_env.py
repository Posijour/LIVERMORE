import os


REQUIRED_ENV_VARS = (
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "TELEGRAM_TOKEN",
    "TELEGRAM_MAIN_CHAT_ID",
    "TELEGRAM_ALERT_CHAT_ID",
)


def _is_missing(value: str | None) -> bool:
    return value is None or value.strip() == ""


def validate_required_env() -> None:
    missing = [name for name in REQUIRED_ENV_VARS if _is_missing(os.getenv(name))]

    invalid = []
    for name in ("TELEGRAM_MAIN_CHAT_ID", "TELEGRAM_ALERT_CHAT_ID"):
        value = os.getenv(name)
        if _is_missing(value):
            continue
        try:
            int(str(value).strip())
        except ValueError:
            invalid.append(name)

    errors = []
    if missing:
        errors.append(f"missing: {', '.join(missing)}")
    if invalid:
        errors.append(f"must be integers: {', '.join(invalid)}")

    if errors:
        joined = "; ".join(errors)
        raise RuntimeError(f"Environment validation failed ({joined})")
