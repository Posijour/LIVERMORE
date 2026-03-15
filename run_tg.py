import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlsplit

from data.queries import load_latest_log_ts
from runtime_env import validate_required_env
from tg.bot import run_bot


SERVICE_NAME = "livermore-core-bot"
logger = logging.getLogger(__name__)


class ServiceState:
    def __init__(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self._lock = threading.Lock()
        self.bot_loop_status = "starting"
        self.bot_last_error: Optional[str] = None
        self.supabase_status = "unknown"
        self.supabase_last_ok_at: Optional[str] = None
        self.supabase_last_error: Optional[str] = None
        self.supabase_last_probe_at: Optional[str] = None
        self.fatal_error: Optional[str] = None
        self.shutting_down = False

    def to_health(self) -> dict:
        now = datetime.now(timezone.utc)
        with self._lock:
            started_at = self.started_at
            bot_loop_status = self.bot_loop_status
            bot_last_error = self.bot_last_error
            supabase_status = self.supabase_status
            supabase_last_probe_at = self.supabase_last_probe_at
            supabase_last_ok_at = self.supabase_last_ok_at
            supabase_last_error = self.supabase_last_error
            shutting_down = self.shutting_down
            fatal_error = self.fatal_error

        is_healthy = self._is_healthy_values(bot_loop_status, supabase_status, shutting_down, fatal_error)

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "service": SERVICE_NAME,
            "started_at": started_at.isoformat(),
            "uptime_sec": int((now - started_at).total_seconds()),
            "bot_loop": {
                "status": bot_loop_status,
                "last_error": bot_last_error,
            },
            "supabase": {
                "status": supabase_status,
                "last_probe_at": supabase_last_probe_at,
                "last_ok_at": supabase_last_ok_at,
                "last_error": supabase_last_error,
            },
            "shutting_down": shutting_down,
            "fatal_error": fatal_error,
        }

    def is_healthy(self) -> bool:
        with self._lock:
            return self._is_healthy_values(
                self.bot_loop_status,
                self.supabase_status,
                self.shutting_down,
                self.fatal_error,
            )

    @staticmethod
    def _is_healthy_values(bot_loop_status: str, supabase_status: str, shutting_down: bool, fatal_error: Optional[str]) -> bool:
        if fatal_error or shutting_down:
            return False
        if bot_loop_status not in {"running", "starting", "restarting"}:
            return False
        if supabase_status == "error":
            return False
        return True

    def set_bot_status(self, status: str, error: Optional[str]) -> None:
        with self._lock:
            self.bot_loop_status = status
            if error:
                self.bot_last_error = error

    def set_supabase_ok(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.supabase_status = "ok"
            self.supabase_last_probe_at = now
            self.supabase_last_ok_at = now
            self.supabase_last_error = None

    def set_supabase_error(self, error: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.supabase_status = "error"
            self.supabase_last_probe_at = now
            self.supabase_last_error = error

    def set_fatal(self, error: str) -> None:
        with self._lock:
            self.fatal_error = error

    def mark_shutting_down(self) -> None:
        with self._lock:
            self.shutting_down = True


def build_root_payload() -> dict:
    return {
        "status": "ok",
        "service": SERVICE_NAME,
    }


def build_health_payload(state: ServiceState) -> dict:
    health = state.to_health()
    return {
        "status": health["status"],
        "service": health["service"],
        "bot": health["bot_loop"]["status"],
        "supabase": health["supabase"]["status"],
        "uptime_sec": health["uptime_sec"],
        "details": health,
    }




def create_handler(state: ServiceState):
    class HealthHandler(BaseHTTPRequestHandler):
        def _normalized_path(self) -> str:
            return urlsplit(self.path).path

        def _send_json(self, status_code: int, payload: dict, include_body: bool = True) -> None:
            body = json.dumps(payload).encode("utf-8") if include_body else b""
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

        def _handle_health(self, include_body: bool) -> None:
            path = self._normalized_path()

            if path == "/":
                self._send_json(200, build_root_payload(), include_body=include_body)
                logger.info("health-http served root endpoint")
                return

            if path == "/health":
                payload = build_health_payload(state)
                status_code = 200 if state.is_healthy() else 503
                self._send_json(status_code, payload, include_body=include_body)
                logger.info("health-http served /health status=%s", payload["status"])
                return

            self._send_json(404, {"error": "not_found"}, include_body=include_body)

        def do_GET(self):  # noqa: N802
            self._handle_health(include_body=True)

        def do_HEAD(self):  # noqa: N802
            self._handle_health(include_body=False)

        def log_message(self, fmt, *args):  # noqa: A003
            logger.info("health-http %s", fmt % args)

    return HealthHandler


def supabase_probe_loop(stop_event: threading.Event, state: ServiceState, interval_sec: int = 60) -> None:
    logger.info("Supabase probe loop started")
    while not stop_event.is_set():
        try:
            load_latest_log_ts()
            state.set_supabase_ok()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Supabase probe failed: %s", exc)
            state.set_supabase_error(str(exc))

        stop_event.wait(interval_sec)
    logger.info("Supabase probe loop stopped")


def heartbeat_loop(stop_event: threading.Event, state: ServiceState, interval_sec: int = 60) -> None:
    while not stop_event.is_set():
        health = state.to_health()
        logger.info(
            "Heartbeat: status=%s bot=%s supabase=%s uptime_sec=%s",
            health["status"],
            health["bot_loop"]["status"],
            health["supabase"]["status"],
            health["uptime_sec"],
        )
        stop_event.wait(interval_sec)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logger.info("Starting %s", SERVICE_NAME)

    try:
        validate_required_env()
    except Exception as exc:  # noqa: BLE001
        logger.error("Startup validation failed: %s", exc)
        return 1

    port = int(os.getenv("PORT", "10000"))
    state = ServiceState()
    stop_event = threading.Event()

    def handle_signal(signum, _frame):
        logger.info("Received signal %s, initiating graceful shutdown", signum)
        state.mark_shutting_down()
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    server = ThreadingHTTPServer(("0.0.0.0", port), create_handler(state))
    server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.5}, daemon=True)
    server_thread.start()
    logger.info("Health server started on port %s", port)

    supabase_thread = threading.Thread(target=supabase_probe_loop, args=(stop_event, state), daemon=True)
    supabase_thread.start()

    heartbeat_thread = threading.Thread(target=heartbeat_loop, args=(stop_event, state), daemon=True)
    heartbeat_thread.start()

    bot_thread = threading.Thread(
        target=run_bot,
        kwargs={"stop_event": stop_event, "on_status": state.set_bot_status},
        daemon=True,
    )
    bot_thread.start()

    logger.info("Startup sequence completed successfully")

    exit_code = 0

    try:
        while not stop_event.is_set():
            if not bot_thread.is_alive():
                state.set_fatal("bot loop thread stopped unexpectedly")
                logger.error("Bot loop thread died unexpectedly")
                stop_event.set()
                exit_code = 1
                break
            time.sleep(1)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fatal error in supervisor loop: %s", exc)
        state.set_fatal(str(exc))
        stop_event.set()
        exit_code = 1
    finally:
        logger.info("Graceful shutdown started")
        state.mark_shutting_down()
        stop_event.set()

        server.shutdown()
        server.server_close()

        bot_thread.join(timeout=45)
        if bot_thread.is_alive():
            logger.warning("Telegram bot thread did not stop before timeout")
        supabase_thread.join(timeout=10)
        heartbeat_thread.join(timeout=10)
        server_thread.join(timeout=10)

        logger.info("Shutdown completed")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
