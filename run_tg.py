import sys
import traceback


def _import_run_bot():
    from tg.bot import run_bot

    return run_bot


def main() -> int:
    print("[run_tg] Starting Telegram bot...", flush=True)

    try:
        run_bot = _import_run_bot()
    except ModuleNotFoundError as exc:
        missing_module = exc.name or "unknown"
        print(
            (
                f"[run_tg] Missing dependency: {missing_module}. "
                "Install dependencies first with: pip install -r requirements.txt"
            ),
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc()
        return 1
    except Exception as exc:  # noqa: BLE001 - bootstrap diagnostics
        print(f"[run_tg] Failed to import bot modules: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 1

    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n[run_tg] Stopped by user.", flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001 - runtime diagnostics
        print(f"[run_tg] Bot crashed: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
