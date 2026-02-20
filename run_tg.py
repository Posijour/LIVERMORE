import subprocess
import sys
import traceback
from pathlib import Path


PIP_PACKAGE_BY_MODULE = {
    "telegram": "python-telegram-bot",
}


def _try_install(package_name: str) -> bool:
    print(f"[run_tg] Attempting to install missing dependency: {package_name}", flush=True)
    cmd = [sys.executable, "-m", "pip", "install", package_name]

    try:
        result = subprocess.run(cmd, check=False)
    except OSError as exc:
        print(f"[run_tg] Failed to start pip: {exc}", file=sys.stderr, flush=True)
        return False

    return result.returncode == 0


def _install_from_requirements() -> bool:
    req_path = Path(__file__).with_name("requirements.txt")
    if not req_path.exists():
        return False

    print(f"[run_tg] Attempting to install dependencies from {req_path.name}", flush=True)
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req_path)]

    try:
        result = subprocess.run(cmd, check=False)
    except OSError as exc:
        print(f"[run_tg] Failed to start pip: {exc}", file=sys.stderr, flush=True)
        return False

    return result.returncode == 0


def _import_run_bot():
    from tg.bot import run_bot

    return run_bot


def main() -> int:
    print("[run_tg] Starting Telegram bot...", flush=True)

    try:
        run_bot = _import_run_bot()
    except ModuleNotFoundError as exc:
        missing_module = exc.name or "unknown"
        package_name = PIP_PACKAGE_BY_MODULE.get(missing_module, missing_module)

        installed = _try_install(package_name)
        if not installed:
            installed = _install_from_requirements()

        if installed:
            try:
                run_bot = _import_run_bot()
            except Exception as retry_exc:  # noqa: BLE001
                print(f"[run_tg] Import still failing after install: {retry_exc}", file=sys.stderr, flush=True)
                traceback.print_exc()
                return 1
        else:
            print(
                f"[run_tg] Missing dependency: {missing_module}. Install it with: pip install {package_name}",
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
