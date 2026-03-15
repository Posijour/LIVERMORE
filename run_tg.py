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
DEFAULT_WORK_STALE_THRESHOLD_SEC = 300


def _resolve_work_stale_threshold_sec() -> int:
    raw = os.getenv("WORK_STALE_THRESHOLD_SEC", str(DEFAULT_WORK_STALE_THRESHOLD_SEC))
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "Invalid WORK_STALE_THRESHOLD_SEC=%r; using default=%s",
            raw,
            DEFAULT_WORK_STALE_THRESHOLD_SEC,
        )
        return DEFAULT_WORK_STALE_THRESHOLD_SEC

    if value <= 0:
        logger.warning(
            "Non-positive WORK_STALE_THRESHOLD_SEC=%s; using default=%s",
            value,
            DEFAULT_WORK_STALE_THRESHOLD_SEC,
        )
        return DEFAULT_WORK_STALE_THRESHOLD_SEC

    return value


class ServiceState:
    def __init__(self, work_stale_threshold_sec: int) -> None:
        self.started_at = datetime.now(timezone.utc)
        self._lock = threading.Lock()
        self.work_stale_threshold_sec = work_stale_threshold_sec
        self.bot_loop_status = "starting"
        self.bot_last_error: Optional[str] = None
        self.supabase_status = "unknown"
        self.supabase_last_ok_at: Optional[str] = None
        self.supabase_last_error: Optional[str] = None
        self.supabase_last_probe_at: Optional[str] = None
        self.last_successful_work_ts: Optional[datetime] = None
        self.fatal_error: Optional[str] = None
        self.shutting_down = False
        self._last_logged_health_state: Optional[str] = None

    def _derive_health_state(
        self,
        now: datetime,
        bot_loop_status: str,
        supabase_status: str,
        shutting_down: bool,
        fatal_error: Optional[str],
        last_successful_work_ts: Optional[datetime],
    ) -> tuple[str, Optional[int]]:
        if fatal_error or shutting_down:
            return "unhealthy", None
        if bot_loop_status not in {"running", "starting", "restarting"}:
            return "unhealthy", None
        if supabase_status == "error":
            return "unhealthy", None

        if bot_loop_status in {"starting", "restarting"}:
            return "healthy", None

        age_sec = None
        if last_successful_work_ts is None:
            age_sec = int((now - self.started_at).total_seconds())
        else:
            age_sec = int((now - last_successful_work_ts).total_seconds())

        if age_sec > self.work_stale_threshold_sec:
            return "unhealthy", age_sec

        return "healthy", age_sec

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
            last_successful_work_ts = self.last_successful_work_ts
            shutting_down = self.shutting_down
            fatal_error = self.fatal_error

        health_status, work_age_sec = self._derive_health_state(
            now,
            bot_loop_status,
            supabase_status,
            shutting_down,
            fatal_error,
            last_successful_work_ts,
        )

        return {
            "status": health_status,
            "service": SERVICE_NAME,
            "started_at": started_at.isoformat(),
            "uptime_sec": int((now - started_at).total_seconds()),
            "last_successful_work_ts": (
                last_successful_work_ts.isoformat().replace("+00:00", "Z")
                if last_successful_work_ts is not None
                else None
            ),
            "last_successful_work_age_sec": work_age_sec,
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
        return self.to_health()["status"] == "healthy"

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

    def mark_successful_work(self) -> None:
        with self._lock:
            self.last_successful_work_ts = datetime.now(timezone.utc)

    def log_health_state_transition(self) -> None:
        health = self.to_health()
        current_state = health["status"]
        age_sec = health["last_successful_work_age_sec"]
        bot_status = health["bot_loop"]["status"]

        with self._lock:
            previous_state = self._last_logged_health_state
            if previous_state == current_state:
                return
            self._last_logged_health_state = current_state

        if previous_state is None:
            logger.info("Health state initialized: %s", current_state)
            return

        logger.warning(
            "Health state changed: %s -> %s (bot=%s, stale_age_sec=%s)",
            previous_state,
            current_state,
            bot_status,
            age_sec,
        )


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
        "last_successful_work_ts": health["last_successful_work_ts"],
        "last_successful_work_age_sec": health["last_successful_work_age_sec"],
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
    work_stale_threshold_sec = _resolve_work_stale_threshold_sec()
    state = ServiceState(work_stale_threshold_sec=work_stale_threshold_sec)
    logger.info("Work freshness threshold configured: %ss", work_stale_threshold_sec)
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

    bot_thread = threading.Thread(
        target=run_bot,
        kwargs={
            "stop_event": stop_event,
            "on_status": state.set_bot_status,
            "on_successful_work": state.mark_successful_work,
        },
        daemon=True,
    )
    bot_thread.start()

    logger.info("Startup sequence completed successfully")

    exit_code = 0

    try:
        while not stop_event.is_set():
            state.log_health_state_transition()
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
        server_thread.join(timeout=10)

        logger.info("Shutdown completed")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
