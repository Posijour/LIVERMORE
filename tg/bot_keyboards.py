from telegram import InlineKeyboardButton, InlineKeyboardMarkup


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
    last_action = (context.user_data.get("last_action") or {}).get("action")

    if last_action in {"alerts", "event", "dispersion", "information"}:
        back_label = "Menu"
        back_target = "main:menu"
    else:
        back_label = "⬅ Back"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(back_label, callback_data=back_target),
            InlineKeyboardButton("🔄 Refresh", callback_data="main:refresh"),
        ],
    ])


def help_keyboard():
    return main_menu_keyboard()
