import sys
import traceback


def main() -> int:
    print("[run_tg] Starting Telegram bot...", flush=True)

    try:
        from tg.bot import run_bot
    except ModuleNotFoundError as exc:
        missing_module = exc.name or "unknown"
        print(
            f"[run_tg] Missing dependency: {missing_module}. Install it with: pip install {missing_module}",
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc()
        return 1
    except Exception as exc:  # noqa: BLE001 - we want full bootstrap diagnostics
        print(f"[run_tg] Failed to import bot modules: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 1

    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n[run_tg] Stopped by user.", flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001 - we want full runtime diagnostics
        print(f"[run_tg] Bot crashed: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

