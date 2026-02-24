import asyncio
import logging
import time
from telegram.error import BadRequest, NetworkError, TimedOut
from config import DATA_SCOPE
from trend.dispersion import compute_dispersion
from trend.event_anchored import event_anchored_analysis
from time_utils import parse_window
from data.queries import load_divergence
from main import persist_snapshot_state, run_snapshot
from persistence.state_history import get_state_persistence_hours
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from trend.state_evolution import analyze_state_evolution

MAX_TELEGRAM_TEXT_LEN = 4000

# --- local anti-spam memory ---
_LAST_ALERTS = {}  # (symbol, div_type) -> event_ts
ALERT_COOLDOWN = 30 * 60  # 30 минут


# ---------------- CONFIG ----------------

TELEGRAM_TOKEN = "8473159744:AAFokOIhOXg9O9qzPYwtTYkdGcROddbToaQ"


SUPPORTED_TICKERS = {
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DOGEUSDT",
    "BCHUSDT",
    "ADAUSDT",
    "HYPEUSDT",
    "XMRUSDT",
    "LINKUSDT",
    "XLMUSDT",
    "LTCUSDT",
    "HBARUSDT",
    "ZECUSDT",
}


def normalize_ticker(raw: str) -> str:
    ticker = raw.strip().upper()
    if not ticker.endswith("USDT"):
        ticker = f"{ticker}USDT"
    return ticker

def help_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="help:stats")],
        [InlineKeyboardButton("🧠 Status (ticker)", callback_data="help:status")],
        [InlineKeyboardButton("⚠️ Alerts", callback_data="run:alerts")],
        [InlineKeyboardButton("📍 Event", callback_data="run:event")],
        [InlineKeyboardButton("📈 Dispersion", callback_data="run:dispersion")],
        [InlineKeyboardButton("🌐 Context", callback_data="run:context")],
    ])

async def lock_menu(query, text="⏳ Loading…"):
    """
    Убирает inline-меню и показывает статус выполнения
    """
    try:
        await query.edit_message_text(text)
    except BadRequest:
        # сообщение могло быть уже изменено или удалено
        pass


# ---------------- HELPERS ----------------

def snapshot_to_text(snapshot):
    r = snapshot.risk or {}
    o = snapshot.options or {}
    v = snapshot.deribit or {}
    d = getattr(snapshot, "divergence", {}) or {}

    return (
        f"Risk: {r.get('avg_risk', 0):.2f} | "
        f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}%\n"
        f"Struct: {o.get('dominant_phase_pct', 0):.1f}% "
        f"{o.get('dominant_phase')}\n"
        f"Vol: {v.get('vbi_state')} "
        f"({v.get('iv_slope_avg', 0):+.2f})\n"
        f"Div: {d.get('count', 0)} | "
        f"{d.get('share', 0)}% | "
        f"{d.get('dominant_type')}"
    )




def _format_hours(hours: int) -> str:
    return f"{hours}h"


def _format_threshold_persistence(label: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    if value == "1":
        return f"{label}: {_format_hours(hours)}"
    return f"{label}: inactive"


def _format_named_state_persistence(label: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    return f"{label}: {value} for {_format_hours(hours)}"


def build_persistence_block() -> str:
    risk_gt_07 = get_state_persistence_hours("risk", "avg_risk_gt_0_7", symbol=None)
    risk_gt_10 = get_state_persistence_hours("risk", "avg_risk_gt_1_0", symbol=None)
    riskact_gt_20 = get_state_persistence_hours("risk", "risk_2plus_pct_gt_20", symbol=None)
    riskact_gt_30 = get_state_persistence_hours("risk", "risk_2plus_pct_gt_30", symbol=None)
    struct_state = get_state_persistence_hours("structure", "dominant_phase", symbol=None)
    vol_state = get_state_persistence_hours("volatility", "vbi_state", symbol=None)

    lines = ["Persistence:"]
    lines.append(_format_threshold_persistence("Risk >0.7", risk_gt_07))
    lines.append(_format_threshold_persistence("Risk >1.0", risk_gt_10))
    lines.append(_format_threshold_persistence("RiskAct >20%", riskact_gt_20))
    lines.append(_format_threshold_persistence("RiskAct >30%", riskact_gt_30))
    lines.append(_format_named_state_persistence("Struct", struct_state))
    lines.append(_format_named_state_persistence("Vol", vol_state))
    return "\n".join(lines)

def split_text_chunks(text: str, chunk_size: int = MAX_TELEGRAM_TEXT_LEN):
    lines = text.splitlines(keepends=True)
    chunks = []
    current = ""

    for line in lines:
        if len(line) > chunk_size:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i:i + chunk_size])
            continue

        if len(current) + len(line) > chunk_size:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    return chunks or [""]


async def safe_reply(update: Update, text: str):
    target = None

    if update.message:
        target = update.message
    elif update.callback_query:
        target = update.callback_query.message

    if not target:
        return

    for chunk in split_text_chunks(text):
        try:
            await target.reply_text(chunk)
        except TimedOut:
            await asyncio.sleep(1)
            await target.reply_text(chunk)


async def run_data_task(update: Update, task_name: str, fn, *args, **kwargs):
    try:
        return await asyncio.to_thread(fn, *args, **kwargs)
    except RuntimeError as exc:
        logger.warning("%s failed: %s", task_name, exc)
        await safe_reply(update, "Data source timeout. Try again in 1-2 minutes.")
    except Exception:
        logger.exception("%s failed", task_name)
        await safe_reply(update, "Temporary data processing error. Try again in 1-2 minutes.")
    return None


async def collect_snapshots(update: Update, task_name: str, windows: list[str], symbol: str | None = None):
    ranges = [(w, *parse_window(w)) for w in windows]

    try:
        snaps = await asyncio.gather(
            *[
                asyncio.to_thread(run_snapshot, ts_from, ts_to, symbol=symbol)
                for _, ts_from, ts_to in ranges
            ]
        )
    except RuntimeError as exc:
        logger.warning("%s failed: %s", task_name, exc)
        await safe_reply(update, "Data source timeout. Try again in 1-2 minutes.")
        return None
    except Exception:
        logger.exception("%s failed", task_name)
        await safe_reply(update, "Temporary data processing error. Try again in 1-2 minutes.")
        return None

    return list(zip(windows, snaps))


# ---------------- COMMANDS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        "Market observer online.\n"
        "Use /stats 1h | 6h | 12h\n"
        "Or /stats 12h 6h 1h"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(
            "Available commands:",
            reply_markup=help_keyboard(),
        )
    except TimedOut:
        await asyncio.sleep(1)
        await update.message.reply_text(
            "Available commands:",
            reply_markup=help_keyboard(),
        )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args

    if not args:
        await safe_reply(update, "Usage: /stats 1h or /stats 12h 6h 1h")
        return

    # -------- SINGLE WINDOW --------
    if len(args) == 1:
        window = args[0]
        ts_from, ts_to = parse_window(window)
        snap = await run_data_task(update, "stats snapshot", run_snapshot, ts_from, ts_to)
        if snap is None:
            return

        await safe_reply(
            update,
            f"=== {window} SNAPSHOT ===\n\n"
            + snapshot_to_text(snap)
            + "\n\nInterpretation available in Risk Log channel."
        )
        return

    # -------- MULTI WINDOW (STATE EVOLUTION) --------
    snapshots = await collect_snapshots(update, "state snapshots", args)
    if snapshots is None:
        return

    # Формируем текст
    text = "=== STATE EVOLUTION ===\n\n"

    for label, snap in snapshots:
        text += f"[{label}]\n"
        text += snapshot_to_text(snap) + "\n\n"

    persistence_block = await run_data_task(update, "state persistence", build_persistence_block)
    if persistence_block is not None:
        text += persistence_block + "\n\n"

    await safe_reply(update, text + "Interpretation available in Risk Log channel.")

async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # -------- ACTIVE STATES (1h) --------
    ts_from, ts_to = parse_window("1h")
    snap = await run_data_task(update, "alerts snapshot", run_snapshot, ts_from, ts_to)
    if snap is None:
        return

    text = "=== ACTIVE STATES ===\n"

    if snap.active_states:
        for s in snap.active_states:
            text += f"• {s}\n"
    else:
        text += "• none\n"

    # -------- RECENT DIVERGENCES (2h) --------
    ts_from, ts_to = parse_window("2h")
    div_rows = await run_data_task(update, "alerts divergence", load_divergence, ts_from, ts_to)
    if div_rows is None:
        return

    text += "\n=== RECENT DIVERGENCES (last 2h) ===\n"

    if div_rows:
        counts = {}
    
        for r in div_rows:
            data = r.get("data", {})
            symbol = data.get("symbol", "UNKNOWN")
            div_type = data.get("divergence_type", "UNKNOWN")
    
            key = (symbol, div_type)
            counts[key] = counts.get(key, 0) + 1
    
        for (symbol, div_type), count in counts.items():
            text += f"• {symbol} — {div_type} ({count})\n"
    else:
        text += "• none\n"

    await safe_reply(update, text)


def can_send_alert(symbol, div_type, event_ts):
    """
    event_ts — timestamp дивергенции из Supabase (ms или s, неважно)
    """
    key = (symbol, div_type)

    last_event_ts = _LAST_ALERTS.get(key)
    if last_event_ts and event_ts <= last_event_ts:
        return False

    _LAST_ALERTS[key] = event_ts
    return True

async def event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1. Берём последнюю дивергенцию
    ts_from, ts_to = parse_window("4h")
    rows = await run_data_task(update, "event divergence", load_divergence, ts_from, ts_to)
    if rows is None:
        return

    if not rows:
        await safe_reply(update, "No recent events found.")
        return

    last = rows[-1]
    data = last.get("data", {})
    symbol = data.get("symbol", "UNKNOWN")
    div_type = data.get("divergence_type", "UNKNOWN")
    event_ts = last.get("ts")

    if not event_ts:
        await safe_reply(update, "Event timestamp missing.")
        return

    snaps = await run_data_task(update, "event analysis", event_anchored_analysis, event_ts)
    if snaps is None:
        return

    text = "=== EVENT ANCHORED ANALYSIS ===\n\n"
    text += f"Event:\n{symbol} — {div_type} (PERPS)\n\n"

    # ---------- TICKER (FUTURES) ----------

    def fmt_ticker(title, snap):
        r = snap.risk or {}
        d = getattr(snap, "divergence", {}) or {}

        return (
            f"--- {title} ---\n"
            f"Risk: {r.get('avg_risk', 0):.2f} | "
            f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}% | "
            f"Divs: {d.get('count', 0)}\n\n"
        )

    text += fmt_ticker("BEFORE", snaps["before"])
    text += fmt_ticker("DURING", snaps["during"])
    text += fmt_ticker("AFTER", snaps["after"])

    # ---------- MARKET CONTEXT (BTC / ETH) ----------
    text += "Options context (BTC / ETH)\n\n"

    def fmt_market(title, snap):
        o = snap.options or {}
        v = snap.deribit or {}

        return (
            f"--- {title} ---\n"
            f"Struct: {o.get('dominant_phase')} "
            f"({o.get('dominant_phase_pct', 0)}%)\n"
            f"Vol: {v.get('vbi_state')} "
            f"({v.get('iv_slope_avg', 0):+.2f})\n\n"
        )

    text += fmt_market("BEFORE", snaps["before"])
    text += fmt_market("DURING", snaps["during"])
    text += fmt_market("AFTER", snaps["after"])

    await safe_reply(
        update,
        text
        + "Interpretation available in Risk Log channel."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Usage: /status BTCUSDT")
        return

    symbol = normalize_ticker(context.args[0])
    if symbol not in SUPPORTED_TICKERS:
        await safe_reply(update, "unknown ticker")
        return

    windows = ["12h", "6h", "1h"]

    ticker_snapshots = await collect_snapshots(update, "status ticker snapshots", windows, symbol=symbol)
    if ticker_snapshots is None:
        return

    market_snapshots = await collect_snapshots(update, "status market snapshots", windows)
    if market_snapshots is None:
        return

    text = f"=== STATUS: {symbol} ===\n\n"

    # ---------- TICKER (FUTURES) ----------
    text += "Perps\n"

    for w, snap in ticker_snapshots:
        r = snap.risk or {}
        d = getattr(snap, "divergence", {}) or {}

        text += f"[{w}]\n"
        text += (
            f"Risk: {r.get('avg_risk', 0):.2f} | "
            f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}% | "
            f"Divs: {d.get('count', 0)}\n"
        )

    # ---------- MARKET CONTEXT (BTC / ETH) ----------
    text += "\nOptions (BTC / ETH)\n"

    for w, snap in market_snapshots:
        o = snap.options or {}
        v = snap.deribit or {}

        text += f"[{w}]\n"
        text += (
            f"Struct: {o.get('dominant_phase')} "
            f"({o.get('dominant_phase_pct', 0):.1f}%)\n"
            f"Vol: {v.get('vbi_state')} "
            f"({v.get('iv_slope_avg', 0):+.2f})\n"
        )

    persistence_block = await run_data_task(update, "status persistence", build_persistence_block)
    if persistence_block is not None:
        text += "\n" + persistence_block

    await safe_reply(update, text)


async def dispersion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await run_data_task(update, "dispersion", compute_dispersion)
    if data is None:
        return

    text = "=== WINDOW DISPERSION ===\n\n"

    text += "Risk (multi-symbol):\n"
    text += f"12h ↔ 1h: {data['risk']['12h_1h']}\n"
    text += f"6h  ↔ 1h: {data['risk']['6h_1h']}\n\n"

    text += "Structure (BTC / ETH):\n"
    text += f"12h ↔ 1h: {data['structure']['12h_1h']}\n"
    text += f"6h  ↔ 1h: {data['structure']['6h_1h']}\n\n"

    text += "Volatility (BTC / ETH):\n"
    text += f"12h ↔ 1h: {data['volatility']['12h_1h']}\n"
    text += f"6h  ↔ 1h: {data['volatility']['6h_1h']}"

    await safe_reply(update, text)


logger = logging.getLogger(__name__)


async def divergence_watcher(app):
    from data.queries import load_divergence
    from time_utils import parse_window

    cycle_from, cycle_to = parse_window("1h")

    market_snapshot = await asyncio.to_thread(run_snapshot, cycle_from, cycle_to)
    await asyncio.to_thread(persist_snapshot_state, market_snapshot, None)

    ts_from, ts_to = parse_window("2h")
    rows = await asyncio.to_thread(load_divergence, ts_from, ts_to)

    for r in rows:
        data = r.get("data", {})
        symbol = data.get("symbol")
        div_type = data.get("divergence_type")
        event_ts = r.get("ts") or r.get("created_at")

        if not symbol or not div_type:
            continue

        if not can_send_alert(symbol, div_type, event_ts):
            continue

        text = (
            "⚠️ Divergence detected\n"
            f"{symbol} — {div_type}"
        )

        # ❗️ ВСТАВЬ СВОЙ CHAT_ID
        await app.bot.send_message(
            chat_id=766363011,
            text=text
        )


async def divergence_watcher_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await divergence_watcher(context.application)
    except TimedOut:
        logger.warning("Watcher send timeout; will retry on next cycle")
    except NetworkError as exc:
        logger.warning("Watcher network error: %s", exc)
    except RuntimeError as exc:
        logger.warning("Watcher data fetch error: %s", exc)
    except Exception:
        logger.exception("Watcher error")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Telegram update handling failed", exc_info=context.error)

async def context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        "Market context is published daily\n"
        "in the Risk Log channel."
    )

def format_scope():
    lines = ["Scope:"]
    lines.append(f"• Futures: {DATA_SCOPE['futures']}")
    lines.append(f"• Options: {DATA_SCOPE['options']}")
    lines.append(f"• Vol: {DATA_SCOPE['vol']}")
    return "\n".join(lines)

async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "help:back":
        await query.edit_message_text(
            "Available commands:",
            reply_markup=help_keyboard(),
        )
        return

    # ---------- STATS ----------
    if data == "help:stats":
        keyboard = [
            [
                InlineKeyboardButton("1h", callback_data="stats:1h"),
                InlineKeyboardButton("6h", callback_data="stats:6h"),
                InlineKeyboardButton("12h", callback_data="stats:12h"),
            ],
            [
                InlineKeyboardButton(
                    "12h → 6h → 1h",
                    callback_data="stats:12h,6h,1h",
                )
            ],
            [
                InlineKeyboardButton("⬅ Back", callback_data="help:back"),
            ],
        ]
        await query.edit_message_text(
            "Stats windows:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("stats:"):
        await lock_menu(query, "⏳ Loading stats…")
        windows = data.split(":", 1)[1].split(",")
        context.args = windows
        await stats(update, context)
        return

    # ---------- STATUS ----------
    if data == "help:status":
        keyboard = [
            [
                InlineKeyboardButton(
                    t.replace("USDT", ""),
                    callback_data=f"status:{t}",
                )
            ]
            for t in sorted(SUPPORTED_TICKERS)
        ]
        keyboard.append(
            [InlineKeyboardButton("⬅ Back", callback_data="help:back")]
        )
        await query.edit_message_text(
            "Select ticker:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("status:"):
        await lock_menu(query, "⏳ Loading status…")
        symbol = data.split(":", 1)[1]
        context.args = [symbol]
        await status(update, context)
        return

    # ---------- DIRECT RUN ----------
    if data == "run:alerts":
        await lock_menu(query, "⏳ Loading alerts…")
        await alerts(update, context)
        return
    elif data == "run:event":
        await lock_menu(query, "⏳ Loading event…")
        await event(update, context)
        return
    elif data == "run:dispersion":
        await lock_menu(query, "⏳ Loading dispersion…")
        await dispersion(update, context)
        return
    elif data == "run:context":
        await lock_menu(query, "⏳ Loading context…")
        await context(update, context)
        return

# ---------------- RUN ----------------

def run_bot():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logger.info("Starting Telegram bot process")

    while True:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_cmd))
        app.add_handler(CommandHandler("stats", stats))
        app.add_handler(CommandHandler("alerts", alerts))
        app.add_handler(CommandHandler("event", event))
        app.add_handler(CommandHandler("status", status))
        app.add_handler(CommandHandler("context", context))
        app.add_handler(CommandHandler("dispersion", dispersion))
        app.add_handler(CallbackQueryHandler(help_callback))
        app.add_error_handler(on_error)

        # ✅ правильный запуск фонового watcher
        app.job_queue.run_repeating(
            divergence_watcher_job,
            interval=120,
            first=10,
        )

        print("Telegram bot running...", flush=True)

        try:
            app.run_polling(close_loop=False)
        except KeyboardInterrupt:
            logger.info("Bot interrupted by user")
            break
        except (TimedOut, NetworkError) as exc:
            logger.warning("Polling interrupted due to network issue: %s", exc)
        except Exception:
            logger.exception("Polling crashed with unexpected error")
        finally:
            logger.warning("Polling stopped. Restarting in 5 seconds...")
            print("Telegram bot polling stopped.", flush=True)
            time.sleep(5)
