import time
import asyncio
from typing import Optional
from trend.event_anchored import event_anchored_analysis
from time_utils import parse_window
from data.queries import load_divergence
from main import run_snapshot
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from main import run_snapshot
from trend.state_evolution import analyze_state_evolution
from output.console import print_state_evolution

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


# ---------------- COMMANDS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Market observer online.\n"
        "Use /stats 1h | 6h | 12h\n"
        "Or /stats 12h 6h 1h"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/stats 1h\n"
        "/stats 6h\n"
        "/stats 12h\n"
        "/stats 12h 6h 1h"
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args

    if not args:
        await update.message.reply_text("Usage: /stats 1h or /stats 12h 6h 1h")
        return

    # -------- SINGLE WINDOW --------
    if len(args) == 1:
        window = args[0]
        ts_from, ts_to = parse_window(window)
        snap = run_snapshot(ts_from, ts_to)

        await update.message.reply_text(
            f"=== {window} SNAPSHOT ===\n\n"
            + snapshot_to_text(snap)
            + "\n\nInterpretation available in Risk Log channel."
        )

    # -------- MULTI WINDOW (STATE EVOLUTION) --------
    snapshots = []
    for w in args:
        ts_from, ts_to = parse_window(w)
        snapshots.append((w, run_snapshot(ts_from, ts_to)))

    analysis = analyze_state_evolution(snapshots)

    # Формируем текст
    text = "=== STATE EVOLUTION ===\n\n"

    for label, snap in snapshots:
        text += f"[{label}]\n"
        text += snapshot_to_text(snap) + "\n\n"

    text += "=== CHANGES ===\n"
    for k, v in analysis["changes"].items():
        text += f"{k}: {v}\n"

    await update.message.reply_text(
        text + "\nInterpretation available in Risk Log channel."
    )

async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # -------- ACTIVE STATES (1h) --------
    ts_from, ts_to = parse_window("1h")
    snap = run_snapshot(ts_from, ts_to)

    text = "=== ACTIVE STATES ===\n"

    if snap.active_states:
        for s in snap.active_states:
            text += f"• {s}\n"
    else:
        text += "• none\n"

    # -------- RECENT DIVERGENCES (2h) --------
    ts_from, ts_to = parse_window("2h")
    div_rows = load_divergence(ts_from, ts_to)

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

    await update.message.reply_text(text)


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
    rows = load_divergence(ts_from, ts_to)

    if not rows:
        await update.message.reply_text("No recent events found.")
        return

    last = rows[-1]
    data = last.get("data", {})
    symbol = data.get("symbol", "UNKNOWN")
    div_type = data.get("divergence_type", "UNKNOWN")
    event_ts = last.get("ts")

    if not event_ts:
        await update.message.reply_text("Event timestamp missing.")
        return

    snaps = event_anchored_analysis(event_ts)

    text = "=== EVENT ANCHORED ANALYSIS ===\n\n"
    text += f"Event:\n{symbol} — {div_type}\n\n"

    def fmt(title, snap):
        r = snap.risk or {}
        o = snap.options or {}
        v = snap.deribit or {}
        d = getattr(snap, "divergence", {}) or {}
    
        return (
            f"--- {title} ---\n"
            f"Risk: {r.get('avg_risk', 0):.2f} | "
            f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}%\n"
            f"Struct: {o.get('dominant_phase')} "
            f"({o.get('dominant_phase_pct', 0)}%)\n"
            f"Vol: {v.get('vbi_state')} "
            f"({v.get('iv_slope_avg', 0):+.2f})\n"
            f"Divs: {d.get('count', 0)}\n\n"
        )

    def fmt_after_with_delta(after, before):
        ra = after.risk or {}
        rb = before.risk or {}
    
        va = after.deribit or {}
        vb = before.deribit or {}
    
        da = getattr(after, "divergence", {}) or {}
        db = getattr(before, "divergence", {}) or {}
    
        def d(x, y):
            return x - y
    
        return (
            "--- AFTER ---\n"
            f"Risk: {ra.get('avg_risk', 0):.2f} | "
            f"RiskAct: {ra.get('risk_2plus_pct', 0):.1f}%\n"
            f"Struct: {after.options.get('dominant_phase')} "
            f"({after.options.get('dominant_phase_pct', 0)}%)\n"
            f"Vol: {va.get('vbi_state')} "
            f"({va.get('iv_slope_avg', 0):+.2f})\n"
            f"Divs: {da.get('count', 0)}\n\n"
            "Δ vs BEFORE:\n"
            f"Risk: {ra.get('avg_risk', 0) - rb.get('avg_risk', 0):+.2f}\n"
            f"RiskAct: {ra.get('risk_2plus_pct', 0) - rb.get('risk_2plus_pct', 0):+.1f}%\n"
            f"Vol: {va.get('iv_slope_avg', 0) - vb.get('iv_slope_avg', 0):+.2f}\n"
            f"Divs: {da.get('count', 0) - db.get('count', 0):+}\n\n"
        )

    text += fmt("BEFORE", snaps["before"])
    text += fmt("DURING", snaps["during"])
    text += fmt_after_with_delta(
        snaps["after"],
        snaps["before"],
    )

    await update.message.reply_text(
        text + "Interpretation available in Risk Log channel."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /status BTCUSDT")
        return

    symbol = normalize_ticker(context.args[0])
    if symbol not in SUPPORTED_TICKERS:
        await update.message.reply_text("unknown ticker")
        return

    windows = ["12h", "6h", "1h"]

    text = f"=== STATUS: {symbol} ===\n\n"

    for w in windows:
        ts_from, ts_to = parse_window(w)
        snap = run_snapshot(ts_from, ts_to, symbol=symbol)

        text += f"[{w}]\n"
        text += snapshot_status_text(snap)
        text += "\n"

    if snap.active_states:
        text += "\nActive states:\n"
        for s in snap.active_states:
            text += f"• {s}\n"

    await update.message.reply_text(text)


def snapshot_status_text(snap):
    r = snap.risk or {}
    o = snap.options or {}
    v = snap.deribit or {}
    d = getattr(snap, "divergence", {}) or {}

    lines = []

    if r:
        lines.append(
            f"Risk: {r.get('avg_risk', 0):.2f} | "
            f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}%"
        )
    else:
        lines.append("Risk: N/A")

    if o:
        lines.append(
            f"Struct: {o.get('dominant_phase')} "
            f"({o.get('dominant_phase_pct', 0)}%)"
        )
    else:
        lines.append("Struct: N/A")

    if v:
        lines.append(
            f"Vol: {v.get('vbi_state')} "
            f"({v.get('iv_slope_avg', 0):+.2f})"
        )
    else:
        lines.append("Vol: N/A")

    lines.append(f"Divs: {d.get('count', 0)}")

    return "\n".join(lines) + "\n"


async def divergence_watcher(app):
    from data.queries import load_divergence
    from time_utils import parse_window

    while True:
        try:
            ts_from, ts_to = parse_window("2h")
            rows = load_divergence(ts_from, ts_to)

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

        except Exception as e:
            print("Watcher error:", e)

        await asyncio.sleep(120)  # 2 минуты

async def context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Market context is published daily\n"
        "in the Risk Log channel."
    )

# ---------------- RUN ----------------

def run_bot():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("alerts", alerts))
    app.add_handler(CommandHandler("event", event))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("context", context))

    # ✅ правильный запуск фонового watcher
    app.job_queue.run_repeating(
        lambda _: divergence_watcher(app),
        interval=120,
        first=10,
    )

    print("Telegram bot running...", flush=True)
    app.run_polling()
    print("Telegram bot polling stopped.", flush=True
