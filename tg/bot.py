import asyncio
import html
import logging
import time
from typing import Callable, Optional
from telegram.error import BadRequest, NetworkError, TimedOut
from config import DATA_SCOPE
from trend.dispersion import compute_dispersion
from trend.market_structure import compute_market_structure
from data.queries import (
    load_bybit_market_state,
    load_deribit,
    load_divergence,
    load_event,
    load_latest_log_ts,
    load_okx_market_state,
    load_risk,
)
from main import persist_snapshot_state, run_snapshot
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from tg.bot_alerts import can_send_alert, can_send_anomaly, detect_buildup_anomalies
from tg.bot_config import ALERT_CHAT_ID, SUPPORTED_TICKERS, TELEGRAM_TOKEN, normalize_ticker
from tg.bot_formatting import (
    _extract_iv_slope,
    _fmt_number,
    _fmt_price,
    _fmt_text,
    _format_last_snapshot_utc,
    _extract_status_price,
    aggregate_options_snapshot,
    build_market_persistence_block_cached,
    parse_window_safe,
    render_options_snapshot,
    snapshot_to_text,
    split_text_chunks,
)
from tg.bot_keyboards import (
    main_menu_keyboard,
    menu_button_keyboard,
    section_nav_keyboard,
    start_inline_keyboard,
)


# --- local anti-spam memory ---


# ---------------- CONFIG ----------------


async def build_info_text(update: Update) -> str:
    latest_ts = await run_data_task(update, "latest log ts", load_latest_log_ts)
    last_snapshot = _format_last_snapshot_utc(latest_ts)

    return (
        "<b>LIVERMORE STRUCTURE CONSOLE</b>\n\n"
        "————————————————\n"
        "system status: online\n"
        f"last snapshot: {last_snapshot}\n"
        "coverage: futures / options / volatility\n"
        "————————————————\n\n"
        "Diagnostic console exposing structural market signals.\n\n"
        "Observed layers:\n"
        "• Futures positioning (crowd risk, divergences)\n"
        "• Options volatility regime (BTC / ETH)\n"
        "• Multi-window snapshots: 12h / 6h / 1h\n"
        "• Regime persistence\n\n"
        "Output:\n"
        "Structure diagnostics only.\n"
        "No trading signals.\n\n"
        "Docs:\n"
        "<code>https://www.notion.so/Livermore-Market-Structure-Monitoring-System-31e600b586bc80acb2cecdfdf1f413df</code>"
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


async def safe_reply(
    update: Update,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str | None = None,
    disable_web_page_preview: bool | None = None,
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
            await query.edit_message_text(
                chunks[0],
                reply_markup=first_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
        except BadRequest:
            await target.reply_text(
                chunks[0],
                reply_markup=first_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
        except TimedOut:
            await asyncio.sleep(1)
            await query.edit_message_text(
                chunks[0],
                reply_markup=first_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )

        for idx, chunk in enumerate(chunks[1:], start=1):
            current_markup = markup if idx == len(chunks) - 1 else None
            try:
                await target.reply_text(
                    chunk,
                    reply_markup=current_markup,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
            except TimedOut:
                await asyncio.sleep(1)
                await target.reply_text(
                    chunk,
                    reply_markup=current_markup,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
        return

    for idx, chunk in enumerate(chunks):
        current_markup = markup if idx == len(chunks) - 1 else None
        try:
            await target.reply_text(
                chunk,
                reply_markup=current_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
        except TimedOut:
            await asyncio.sleep(1)
            await target.reply_text(
                chunk,
                reply_markup=current_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )


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
    ranges = []
    for w in windows:
        bounds = parse_window_safe(w)
        if bounds is None:
            await safe_reply(update, f"Invalid window: {w}. Use values like 10m, 1h, 6h, 1d.")
            return None
        ranges.append((w, *bounds))

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
    text = await build_info_text(update)

    await update.message.reply_text(
        text,
        reply_markup=section_nav_keyboard(context),
        parse_mode=ParseMode.HTML,
    )


async def information(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_last_action(context, "information")
    text = await build_info_text(update)
    await safe_reply(
        update,
        text,
        reply_markup=section_nav_keyboard(context),
        parse_mode=ParseMode.HTML,
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    remember_last_action(context, "stats", args)

    if not args:
        await safe_reply(update, "Usage: /stats 1h or /stats 12h 6h 1h")
        return

    # -------- SINGLE WINDOW --------
    if len(args) == 1:
        window = args[0]
        bounds = parse_window_safe(window)
        if bounds is None:
            await safe_reply(update, "Invalid window. Use values like 10m, 1h, 6h, 1d.")
            return
        ts_from, ts_to = bounds
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

    persistence_block = await run_data_task(update, "state persistence", build_market_persistence_block_cached)
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

    bounds = parse_window_safe(window)
    if bounds is None:
        await safe_reply(update, "Usage: /options [1h|6h|12h]")
        return
    ts_from, ts_to = bounds

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
    ts_from, ts_to = parse_window_safe("1h")
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
    ts_from, ts_to = parse_window_safe("2h")
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


async def event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from trend.event_anchored import event_anchored_analysis
    remember_last_action(context, "event")
    # 1. Берём последнюю дивергенцию
    ts_from, ts_to = parse_window_safe("4h")
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
            f"({_extract_iv_slope(v):+.2f})\n\n"
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

    ts_from, ts_to = parse_window_safe("10m")
    risk_rows = await run_data_task(update, "status price (10m)", load_risk, ts_from, ts_to, symbol)
    if risk_rows is None:
        return

    if not risk_rows:
        ts_from, ts_to = parse_window_safe("30m")
        risk_rows = await run_data_task(update, "status price (1h fallback)", load_risk, ts_from, ts_to, symbol)
        if risk_rows is None:
            return

    price_value = _extract_status_price(risk_rows)
    symbol_html = html.escape(symbol)

    text = f"=== STATUS: {symbol_html} ===\n\n"
    text += f"Price: {_fmt_price(price_value)}\n\n"


    # ---------- TICKER (FUTURES) ----------
    text += "<u>Perps</u>\n"

    for w, snap in ticker_snapshots:
        r = snap.risk or {}
        d = getattr(snap, "divergence", {}) or {}

        text += f"<b>[{html.escape(w)}]</b>\n"
        text += (
            f"Risk: {r.get('avg_risk', 0):.2f} | "
            f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}% | "
            f"Divs: {d.get('count', 0)}\n"
        )

    # ---------- MARKET CONTEXT (BTC / ETH) ----------
    text += "\n<u>Options (BTC / ETH)</u>\n"

    for w, snap in market_snapshots:
        o = snap.options or {}
        v = snap.deribit or {}

        text += f"<b>[{html.escape(w)}]</b>\n"
        text += (
            f"Struct: {html.escape(str(o.get('dominant_phase')))} "
            f"({o.get('dominant_phase_pct', 0):.1f}%)\n"
            f"Vol: {html.escape(str(v.get('vbi_state')))} "
            f"({_extract_iv_slope(v):+.2f})\n"
        )

    persistence_block = await run_data_task(update, "status persistence", build_market_persistence_block_cached)
    if persistence_block is not None:
        persistence_block = html.escape(persistence_block)
        persistence_block = persistence_block.replace("Market Persistence:", "<u>Market Persistence</u>:")
        text += "\n" + persistence_block

    await safe_reply(update, text, reply_markup=section_nav_keyboard(context), parse_mode=ParseMode.HTML)


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

        # -------- MARKET STRUCTURE (extra block) --------
    ms = await run_data_task(
        update,
        "market structure",
        compute_market_structure,
        sorted(SUPPORTED_TICKERS),
        12,  # lookback hours for adaptive normalization
    )

    if ms:
        text += "\n\n=== MARKET STRUCTURE ===\n\n"
        text += f"Coherence: {ms.get('coherence_label')} ({_fmt_number(ms.get('coherence'), 2)})\n"
        text += f"Compression: {ms.get('compression_label')} ({_fmt_number(ms.get('compression_score'), 2)})\n"
        text += f"Driver: {_fmt_text(ms.get('driver'))}\n"
        text += f"Regime: {_fmt_text(ms.get('regime'))}"

    await safe_reply(update, text, reply_markup=section_nav_keyboard(context))


logger = logging.getLogger(__name__)
_PERSISTENCE_WARNING_EMITTED = False


async def divergence_watcher(app):
    global _PERSISTENCE_WARNING_EMITTED
    from data.queries import load_divergence

    cycle_from, cycle_to = parse_window_safe("1h")

    market_snapshot = await asyncio.to_thread(run_snapshot, cycle_from, cycle_to)
    try:
        await asyncio.to_thread(persist_snapshot_state, market_snapshot, None)
    except Exception as exc:
        if isinstance(exc, NameError) and "ts" in str(exc):
            if not _PERSISTENCE_WARNING_EMITTED:
                logger.error(
                    "Snapshot persistence code is outdated (NameError: ts). "
                    "Deploy latest main.py and restart bot process."
                )
                _PERSISTENCE_WARNING_EMITTED = True
        else:
            logger.warning("Snapshot persistence failed in watcher: %s", exc)

    ts_from, ts_to = parse_window_safe("2h")
    rows = await asyncio.to_thread(load_divergence, ts_from, ts_to)

        # ---------- FUTURES ANOMALIES (BUILDUP-BASED) ----------
    a_from, a_to = parse_window_safe("30m")
    alert_rows = await asyncio.to_thread(load_event, "alert_sent", a_from, a_to)

    anomalies = detect_buildup_anomalies(alert_rows)

    for anomaly in anomalies:
        if not can_send_anomaly(anomaly["key"], anomaly["event_ts"]):
            continue

        try:
            await app.bot.send_message(
                chat_id=ALERT_CHAT_ID,
                text=anomaly["text"],
            )
        except BadRequest as exc:
            logger.error(
                "Watcher failed to deliver anomaly alert to chat %s: %s",
                ALERT_CHAT_ID,
                exc,
            )

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
        try:
            await app.bot.send_message(
                chat_id=ALERT_CHAT_ID,
                text=text
            )
        except BadRequest as exc:
            logger.error("Watcher failed to deliver alert to chat %s: %s", ALERT_CHAT_ID, exc)


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

def _build_application():
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

    if app.job_queue is None:
        logger.warning("JobQueue is unavailable; watcher job is disabled")
    else:
        app.job_queue.run_repeating(
            divergence_watcher_job,
            interval=120,
            first=10,
        )
    return app


def run_bot(
    stop_event=None,
    on_status: Optional[Callable[[str, Optional[str]], None]] = None,
    initial_backoff_seconds: float = 3.0,
    max_backoff_seconds: float = 30.0,
):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logger.info("Starting Telegram bot process")

    backoff_seconds = initial_backoff_seconds

    while True:
        if stop_event is not None and stop_event.is_set():
            logger.info("Stop requested before bot startup")
            if on_status:
                on_status("stopped", None)
            break

        app = _build_application()

        try:
            if on_status:
                on_status("starting", None)

            app.initialize()
            app.start()
            if app.updater is None:
                raise RuntimeError("Application updater is not initialized")
            app.updater.start_polling(drop_pending_updates=False)
            logger.info("Telegram polling started")
            print("Telegram bot running...", flush=True)

            if on_status:
                on_status("running", None)

            while True:
                if stop_event is not None and stop_event.is_set():
                    logger.info("Stop signal received for Telegram bot")
                    if on_status:
                        on_status("stopping", None)
                    break
                time.sleep(1)

            backoff_seconds = initial_backoff_seconds

            if stop_event is not None and stop_event.is_set():
                if on_status:
                    on_status("stopped", None)
                break
        except KeyboardInterrupt:
            logger.info("Bot interrupted by user")
            if on_status:
                on_status("stopped", None)
            break
        except (TimedOut, NetworkError) as exc:
            logger.warning("Polling interrupted due to network issue: %s", exc)
            if on_status:
                on_status("restarting", str(exc))
        except Exception as exc:
            logger.exception("Polling crashed with unexpected error")
            if on_status:
                on_status("restarting", str(exc))
        finally:
            try:
                if app.updater is not None:
                    app.updater.stop()
            except Exception:
                logger.exception("Failed to stop Telegram updater cleanly")
            try:
                app.stop()
            except Exception:
                logger.exception("Failed to stop Telegram app cleanly")
            try:
                app.shutdown()
            except Exception:
                logger.exception("Failed to shutdown Telegram app cleanly")

            if stop_event is not None and stop_event.is_set():
                logger.info("Polling stopped due to shutdown")
                print("Telegram bot polling stopped.", flush=True)
                break

            logger.warning("Polling stopped. Restarting in %.1f seconds...", backoff_seconds)
            print("Telegram bot polling stopped.", flush=True)
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)
