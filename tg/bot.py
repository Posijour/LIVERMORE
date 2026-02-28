import asyncio
import logging
import time
from telegram.error import BadRequest, NetworkError, TimedOut
from config import DATA_SCOPE
from trend.dispersion import compute_dispersion
from time_utils import parse_window
from data.queries import load_deribit, load_divergence, load_okx_market_state, load_bybit_market_state
from main import persist_snapshot_state, run_snapshot
from persistence.state_history import get_state_persistence_hours
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
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

def menu_button_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Menu", callback_data="main:menu")],
    ])


def start_inline_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶ Start", callback_data="main:start")],
    ])


def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="help:stats")],
        [InlineKeyboardButton("🧠 Status (ticker)", callback_data="help:status")],
        [InlineKeyboardButton("🧩 Options", callback_data="help:options")],
        [InlineKeyboardButton("⚠️ Alerts", callback_data="run:alerts")],
        [InlineKeyboardButton("📍 Event", callback_data="run:event")],
        [InlineKeyboardButton("📈 Dispersion", callback_data="run:dispersion")],
        [InlineKeyboardButton("ℹ️ Information", callback_data="run:information")],
    ])


def section_nav_keyboard(context):
    back_target = context.user_data.get("back_target", "main:menu")

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⬅ Back", callback_data=back_target),
            InlineKeyboardButton("🔄 Refresh", callback_data="main:refresh"),
        ],
    ])

def help_keyboard():
    return main_menu_keyboard()


INFO_TEXT = (
    "This bot is a diagnostic console.\n\n"
    "It exposes different layers of market state:\n\n"
    "• Ticker layer (Futures)\n"
    "  Crowd risk, activity and divergences per symbol\n\n"
    "• Market context (BTC / ETH)\n"
    "  Options structure and volatility regime\n\n"
    "• Time windows\n"
    "  12h / 6h / 1h snapshots\n\n"
    "• Persistence\n"
    "  How long the current market regime has been active\n\n"
    "The bot does not generate signals\n"
    "and does not suggest actions.\n\n"
    "Interpretation is intentionally external."
)

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
        f"({v.get('iv_slope', 0):+.2f})\n"
        f"Div: {d.get('count', 0)} | "
        f"{d.get('share', 0)}% | "
        f"{d.get('dominant_type')}"
    )




def _format_hours(hours: int) -> str:
    return f"{hours}h"


def _humanize_risk_band(state_key: str, value: str) -> str:
    avg_risk_map = {
        "LE_0_7": "≤ 0.7",
        "GT_0_7": "> 0.7",
        "GT_1_0": "> 1.0",
        "NO_DATA": "no data",
    }
    riskact_map = {
        "LE_20": "≤ 20%",
        "GT_20": "> 20%",
        "GT_30": "> 30%",
        "NO_DATA": "no data",
    }

    if state_key == "avg_risk":
        return avg_risk_map.get(value, value)
    if state_key == "risk_2plus_pct":
        return riskact_map.get(value, value)
    return value


def _format_risk_band_persistence(label: str, state_key: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    human_value = _humanize_risk_band(state_key, value)
    return f"{label} {human_value} for {_format_hours(hours)}"


def _format_named_state_persistence(label: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    return f"{label}: {value} for {_format_hours(hours)}"


def build_persistence_block() -> str:
    avg_risk_state = get_state_persistence_hours("risk", "avg_risk", symbol=None)
    riskact_state = get_state_persistence_hours("risk", "risk_2plus_pct", symbol=None)
    struct_state = get_state_persistence_hours("structure", "dominant_phase", symbol=None)
    vol_state = get_state_persistence_hours("volatility", "vbi_state", symbol=None)

    lines = ["Persistence:"]
    lines.append(_format_risk_band_persistence("Risk", "avg_risk", avg_risk_state))
    lines.append(_format_risk_band_persistence("RiskAct", "risk_2plus_pct", riskact_state))
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


def _avg(rows: list[dict], field: str) -> float | None:
    values = [r.get("data", {}).get(field) for r in rows]
    numeric = [float(v) for v in values if isinstance(v, (int, float))]
    if not numeric:
        return None
    return sum(numeric) / len(numeric)


def _mode(rows: list[dict], field: str):
    values = [r.get("data", {}).get(field) for r in rows]
    values = [v for v in values if v not in (None, "")]
    if not values:
        return None
    return max(set(values), key=values.count)


def _latest(rows: list[dict], field: str):
    if not rows:
        return None
    latest_row = max(rows, key=lambda r: r.get("ts", 0))
    return latest_row.get("data", {}).get(field)


def _fmt_number(value, digits: int = 3, signed: bool = False) -> str:
    if value is None or not isinstance(value, (int, float)):
        return "N/A"
    return f"{value:+.{digits}f}" if signed else f"{value:.{digits}f}"


def _fmt_text(value) -> str:
    return "N/A" if value in (None, "") else str(value)


def _derive_term_structure(iv_slope, curvature) -> str:
    if not isinstance(iv_slope, (int, float)) or not isinstance(curvature, (int, float)):
        return "N/A"
    return f"iv_slope={iv_slope:+.3f}, curvature={curvature:+.3f}"


def aggregate_options_snapshot(bybit_rows, okx_rows, deribit_rows) -> dict:
    return {
        "bybit": {
            "regime": _mode(bybit_rows, "regime"),
            "mci": _avg(bybit_rows, "mci"),
            "mci_slope": _avg(bybit_rows, "mci_slope"),
            "mci_phase": _mode(bybit_rows, "mci_phase"),
            "confidence": _latest(bybit_rows, "confidence"),
        },
        "okx": {
            "okx_olsi_avg": _avg(okx_rows, "okx_olsi_avg"),
            "okx_olsi_slope": _latest(okx_rows, "okx_olsi_slope"),
            "okx_liquidity_regime": _mode(okx_rows, "okx_liquidity_regime"),
            "divergence": _mode(okx_rows, "divergence_type"),
            "divergence_diff": _avg(okx_rows, "divergence_diff"),
            "divergence_strength": _avg(okx_rows, "divergence_strength"),
        },
        "deribit": {
            "vbi_state": _mode(deribit_rows, "vbi_state"),
            "iv_slope": _latest(deribit_rows, "iv_slope"),
            "curvature": _avg(deribit_rows, "curvature"),
            "skew": _latest(deribit_rows, "skew"),
        },
    }


def render_options_snapshot(window: str, payload: dict) -> str:
    bybit = payload.get("bybit", {})
    okx = payload.get("okx", {})
    deribit = payload.get("deribit", {})

    # ---- helpers ----
    def arrow(value):
        if not isinstance(value, (int, float)):
            return ""
        return "↑" if value > 0 else "↓" if value < 0 else "→"

    def liquidity_label(phase):
        if phase in ("THIN", "LOW"):
            return "THIN"
        if phase in ("RICH", "HIGH"):
            return "RICH"
        return _fmt_text(phase)

    iv_slope = deribit.get("iv_slope")

    term_structure = (
        "flat"
        if isinstance(iv_slope, (int, float)) and abs(iv_slope) < 0.3
        else "upward"
        if isinstance(iv_slope, (int, float)) and iv_slope > 0
        else "downward"
        if isinstance(iv_slope, (int, float))
        else "N/A"
    )

    confidence_label = (
        "LOW"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.30
        else "WEAK"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.50
        else "MODERATE"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.70
        else "HIGH"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.85
        else "VERY_HIGH"
        if isinstance(bybit.get("confidence"), (int, float))
        else "N/A"
    )

    return (
        f"=== OPTIONS SNAPSHOT ({window}) ===\n\n"

        "Behavior (Bybit):\n"
        f"• Regime: {_fmt_text(bybit.get('regime'))}\n"
        f"• Confidence: {_fmt_number(bybit.get('confidence'), 2)} ({confidence_label})\n"
        f"• MCI: {_fmt_number(bybit.get('mci'), 2)} "
        f"({arrow(bybit.get('mci_slope'))})\n\n"

        "Liquidity (OKX):\n"
        f"• Liquidity: {liquidity_label(okx.get('okx_liquidity_regime'))}\n"
        f"• OLSI: {_fmt_number(okx.get('okx_olsi_avg'), 2)} "
        f"({arrow(okx.get('okx_olsi_slope'))})\n"
        f"• Liquidity phase: {_fmt_text(okx.get('okx_liquidity_regime'))}\n\n"

        "Mismatch (Bybit ↔ OKX):\n"
        f"• {_fmt_text(okx.get('divergence'))}\n"
        f"• Strength: {_fmt_number(okx.get('divergence_strength'), 2)}\n\n"

        "Volatility (Deribit):\n"
        f"• VBI: {_fmt_text(deribit.get('vbi_state'))}\n"
        f"• Term structure: {term_structure}"
    )


async def safe_reply(
    update: Update,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
):
    target = None
    query = update.callback_query

    if update.message:
        target = update.message
    elif query:
        target = query.message

    if not target:
        return

    chunks = split_text_chunks(text)
    markup = reply_markup if reply_markup is not None else menu_button_keyboard()

    # If we came from inline callback, replace loading text in that same message.
    if query and chunks:
        first_markup = markup if len(chunks) == 1 else None
        try:
            await query.edit_message_text(chunks[0], reply_markup=first_markup)
        except BadRequest:
            await target.reply_text(chunks[0], reply_markup=first_markup)
        except TimedOut:
            await asyncio.sleep(1)
            await query.edit_message_text(chunks[0], reply_markup=first_markup)

        for idx, chunk in enumerate(chunks[1:], start=1):
            current_markup = markup if idx == len(chunks) - 1 else None
            try:
                await target.reply_text(chunk, reply_markup=current_markup)
            except TimedOut:
                await asyncio.sleep(1)
                await target.reply_text(chunk, reply_markup=current_markup)
        return

    for idx, chunk in enumerate(chunks):
        current_markup = markup if idx == len(chunks) - 1 else None
        try:
            await target.reply_text(chunk, reply_markup=current_markup)
        except TimedOut:
            await asyncio.sleep(1)
            await target.reply_text(chunk, reply_markup=current_markup)


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
    if not update.message:
        return

    context.user_data["is_started"] = True
    context.user_data["last_action"] = None

    await update.message.reply_text(
        "Market observer online.",
        reply_markup=menu_button_keyboard(),
    )


async def ensure_started(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("is_started"):
        return

    context.user_data["is_started"] = False
    if update.message:
        await update.message.reply_text(
            "Press Start to begin.",
            reply_markup=start_inline_keyboard(),
        )


def remember_last_action(context: ContextTypes.DEFAULT_TYPE, action: str, args: list[str] | None = None):
    context.user_data["last_action"] = {"action": action, "args": args or []}


async def run_last_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    last_action = context.user_data.get("last_action")
    if not last_action:
        await safe_reply(update, "Nothing to refresh yet.", reply_markup=section_nav_keyboard(context))
        return

    action = last_action.get("action")
    args = last_action.get("args", [])
    context.args = list(args)

    if action == "stats":
        await stats(update, context)
    elif action == "status":
        await status(update, context)
    elif action == "options":
        await options(update, context)
    elif action == "alerts":
        await alerts(update, context)
    elif action == "event":
        await event(update, context)
    elif action == "dispersion":
        await dispersion(update, context)
    elif action == "information":
        await information(update, context)
    else:
        await safe_reply(update, "Nothing to refresh yet.", reply_markup=section_nav_keyboard(context))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    await update.message.reply_text(
        "Main menu:",
        reply_markup=main_menu_keyboard(),
    )


async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    remember_last_action(context, "information")

    await update.message.reply_text(
        INFO_TEXT,
        reply_markup=section_nav_keyboard(context),
    )


async def information(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_last_action(context, "information")
    await safe_reply(update, INFO_TEXT, reply_markup=section_nav_keyboard(context))


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    remember_last_action(context, "stats", args)

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
            + "\n\nInterpretation available in Risk Log channel.",
            reply_markup=section_nav_keyboard(context),
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

    await safe_reply(
        update,
        text + "Interpretation available in Risk Log channel.",
        reply_markup=section_nav_keyboard(context),
    )

async def options(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    window = args[0] if args else "1h"
    remember_last_action(context, "options", [window])

    if window not in {"1h", "6h", "12h"}:
        await safe_reply(update, "Usage: /options [1h|6h|12h]")
        return

    ts_from, ts_to = parse_window(window)

    bybit_rows = await run_data_task(
        update,
        "options bybit",
        load_bybit_market_state,
        ts_from,
        ts_to,
    )
    if bybit_rows is None:
        return

    okx_rows = await run_data_task(
        update,
        "options okx",
        load_okx_market_state,
        ts_from,
        ts_to,
    )
    if okx_rows is None:
        return

    deribit_rows = await run_data_task(
        update,
        "options deribit",
        load_deribit,
        ts_from,
        ts_to,
    )
    if deribit_rows is None:
        return

    aggregated = aggregate_options_snapshot(bybit_rows, okx_rows, deribit_rows)
    text = render_options_snapshot(window, aggregated)
    await safe_reply(update, text, reply_markup=section_nav_keyboard(context))


async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_last_action(context, "alerts")
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

    await safe_reply(update, text, reply_markup=section_nav_keyboard(context))


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
    from trend.event_anchored import event_anchored_analysis
    remember_last_action(context, "event")
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
            f"({v.get('iv_slope', 0):+.2f})\n\n"
        )

    text += fmt_market("BEFORE", snaps["before"])
    text += fmt_market("DURING", snaps["during"])
    text += fmt_market("AFTER", snaps["after"])

    await safe_reply(
        update,
        text + "Interpretation available in Risk Log channel.",
        reply_markup=section_nav_keyboard(context),
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_last_action(context, "status", context.args)
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

    await safe_reply(update, text, reply_markup=section_nav_keyboard(context))


async def dispersion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_last_action(context, "dispersion")
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

    await safe_reply(update, text, reply_markup=section_nav_keyboard(context))


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

    if data in {"main:menu", "help:back"}:
        await query.edit_message_text(
            "Main menu:",
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "main:start":
        context.user_data["is_started"] = True
        context.user_data["last_action"] = None
        await query.edit_message_text(
            "Market observer online.",
            reply_markup=menu_button_keyboard(),
        )
        return

    if data == "main:refresh":
        await lock_menu(query, "⏳ Refreshing…")
        await run_last_action(update, context)
        return

    # ---------- STATS ----------
    if data == "help:stats":
        context.user_data["back_target"] = "help:stats"
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
                InlineKeyboardButton("⬅ Back", callback_data="main:menu"),
                InlineKeyboardButton("Menu", callback_data="main:menu"),
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

    # ---------- OPTIONS ----------
    if data == "help:options":
        context.user_data["back_target"] = "help:options"
        keyboard = [
            [
                InlineKeyboardButton("1h", callback_data="options:1h"),
                InlineKeyboardButton("6h", callback_data="options:6h"),
                InlineKeyboardButton("12h", callback_data="options:12h"),
            ],
            [
                InlineKeyboardButton("⬅ Back", callback_data="main:menu"),
                InlineKeyboardButton("Menu", callback_data="main:menu"),
            ],
        ]
        await query.edit_message_text(
            "Options windows:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("options:"):
        await lock_menu(query, "⏳ Loading options…")
        window = data.split(":", 1)[1]
        context.args = [window]
        await options(update, context)
        return

    # ---------- STATUS ----------
    if data == "help:status":
        context.user_data["back_target"] = "help:status"
        tickers = sorted(SUPPORTED_TICKERS)
        keyboard = []
        for idx in range(0, len(tickers), 2):
            row = [
                InlineKeyboardButton(
                    tickers[idx].replace("USDT", ""),
                    callback_data=f"status:{tickers[idx]}",
                )
            ]
            if idx + 1 < len(tickers):
                row.append(
                    InlineKeyboardButton(
                        tickers[idx + 1].replace("USDT", ""),
                        callback_data=f"status:{tickers[idx + 1]}",
                    )
                )
            keyboard.append(row)
        keyboard.append(
            [
                InlineKeyboardButton("⬅ Back", callback_data="main:menu"),
                InlineKeyboardButton("Menu", callback_data="main:menu"),
            ]
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
    if data == "run:event":
        await lock_menu(query, "⏳ Loading event…")
        await event(update, context)
        return
    if data == "run:dispersion":
        await lock_menu(query, "⏳ Loading dispersion…")
        await dispersion(update, context)
        return
    if data == "run:information":
        await lock_menu(query, "⏳ Loading information…")
        await information(update, context)
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
        app.add_handler(CommandHandler(["info", "information"], info_cmd))
        app.add_handler(CommandHandler("stats", stats))
        app.add_handler(CommandHandler("options", options))
        app.add_handler(CommandHandler("alerts", alerts))
        app.add_handler(CommandHandler("event", event))
        app.add_handler(CommandHandler("status", status))
        app.add_handler(CommandHandler("context", info_cmd))
        app.add_handler(CommandHandler("dispersion", dispersion))
        app.add_handler(MessageHandler(filters.ALL, ensure_started))
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











