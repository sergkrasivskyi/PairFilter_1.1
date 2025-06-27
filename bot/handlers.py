"""
bot/handlers.py – v1.3
──────────────────────
• сцена «Парсинг пар» із лічильником;
• /tick, /sync_history, /status;
• inline refresh_yes / refresh_no.

Зміни в 1.3
───────────
• _update_status()  – тепер «Подано пар: N».
• _show_summary()   – додає total_pairs у БД.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from core.parsing import extract_pairs
from db.sqlite import DB
from bot.scheduler import _tick, ask_full_refresh, check_history, backfill

log = logging.getLogger("pairfilter")

PARSE = range(1)  # ID стану

# ────────────────────────── reply-меню ──
def menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Парсинг пар", "Статус", "Оновити ціни", "Синхр. історію"]],
        resize_keyboard=True,
    )

def session_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Завершити", callback_data="finish"),
                InlineKeyboardButton("❌ Скасувати", callback_data="cancel"),
            ]
        ]
    )

# ────────────────────────── /start ──
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 Ласкаво просимо!", reply_markup=menu_keyboard()
    )

# ────────────────────────── сцена «Парсинг пар» ──
async def parsing_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    status_msg = await update.message.reply_text(
        "Пересилайте повідомлення з парами.\nПодано пар: 0",
        reply_markup=session_keyboard(),
    )
    context.chat_data.update(
        {
            "status_id": status_msg.message_id,
            "seen_cnt": 0,
            "added_cnt": 0,
            "added_pairs": set(),
        }
    )
    return PARSE

async def parsing_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pairs = extract_pairs(update.message.text or "")
    if not pairs:
        await _safe_delete(update.message)
        return PARSE

    db: DB = context.application.bot_data["db"]
    for t1, t2 in pairs:
        sym_a, sym_b = sorted([f"{t1}USDT", f"{t2}USDT"])
        context.chat_data["seen_cnt"] += 1
        if await db.add_pair(sym_a, sym_b):
            context.chat_data["added_cnt"] += 1
            context.chat_data["added_pairs"].add((sym_a, sym_b))

    await _update_status(context)
    await _safe_delete(update.message)
    return PARSE

async def parsing_finish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    await _show_summary(context, cancelled=False)
    log.info("Парсинг пар завершено (%s подано, %s нових)",
             context.chat_data.get("seen_cnt"),
             context.chat_data.get("added_cnt"))
    return ConversationHandler.END

async def parsing_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()

    # rollback нових пар
    db: DB = context.application.bot_data["db"]
    for sym_a, sym_b in context.chat_data.get("added_pairs", set()):
        await db.conn.execute(
            "DELETE FROM watched_pairs WHERE sym_a=? AND sym_b=?", (sym_a, sym_b)
        )
    await db.conn.commit()

    await _show_summary(context, cancelled=True)
    log.info("Парсинг пар СКАСОВАНО")
    return ConversationHandler.END

# ────────────────────────── manual /tick ──
async def manual_tick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user.username or update.effective_user.id
    await _tick(context.application, manual_user=str(user), silent=True)
    await update.message.reply_text("✅ Оновлено!", reply_markup=menu_keyboard())

# ────────────────────────── /sync_history ──
async def sync_history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.application.bot_data["db"]
    symbols = await check_history(db)
    if symbols:
        await ask_full_refresh(context.application, symbols)
    else:
        await update.message.reply_text("✅ Історія вже повна й актуальна.")

# ────────────────────────── /status ──
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.application.bot_data["db"]
    pairs = [row async for row in db.iter_pairs()]
    symbols = {s for a, b in pairs for s in (a, b)}

    complete = 0
    async with db.conn.execute(
        f"SELECT symbol, COUNT(*) FROM prices "
        f"WHERE symbol IN ({','.join('?'*len(symbols))}) GROUP BY symbol",
        tuple(symbols),
    ) as cur:
        rows = await cur.fetchall()
    for _, cnt in rows:
        if cnt >= 672:
            complete += 1

    await update.message.reply_text(
        f"📊 Токенів: {len(symbols)}\n"
        f"   • повна історія     : {complete}\n"
        f"   • неповна / застаріла: {len(symbols) - complete}"
    )

# ────────────────────────── callback refresh_yes / no ──
async def refresh_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    if choice == "refresh_yes":
        symbols = context.application.bot_data.get("refresh_tokens", [])
        if not symbols:
            await query.edit_message_text("Список токенів порожній або вже оновлено.")
            return
        db: DB = context.application.bot_data["db"]
        await query.edit_message_text(f"⏳ Починаю бек-філ ({len(symbols)} токенів)…")
        await backfill(db, symbols)
        await query.edit_message_text("✅ Історію оновлено.")
        log.info("бек-філ виконано користувачем %s (%s токенів)",
                 query.from_user.id, len(symbols))
    else:
        await query.edit_message_text("🚫 Бек-філ скасовано.")
        log.info("бек-філ скасовано користувачем %s", query.from_user.id)

# ────────────────────────── helpers ──
async def _update_status(context: CallbackContext) -> None:
    msg_id = context.chat_data.get("status_id")
    if msg_id is None:
        return
    seen = context.chat_data.get("seen_cnt", 0)
    try:
        await context.bot.edit_message_text(
            chat_id=context._chat_id,
            message_id=msg_id,
            text=f"Пересилайте повідомлення з парами.\nПодано пар: {seen}",
            reply_markup=session_keyboard(),
        )
    except Exception:
        pass  # тихо ігноруємо дрібні race-умови

async def _show_summary(context: CallbackContext, *, cancelled: bool) -> None:
    msg_id = context.chat_data.get("status_id")
    if msg_id is None:
        return

    seen = context.chat_data.get("seen_cnt", 0)
    added = context.chat_data.get("added_cnt", 0)

    if cancelled:
        text = "❌ Скасовано. Дані не збережені."
    else:
        # total пар у БД після вставки
        db: DB = context.application.bot_data["db"]
        async with db.conn.execute("SELECT COUNT(*) FROM watched_pairs") as cur:
            total_pairs = (await cur.fetchone())[0]
        text = (
            "✅ Готово!\n"
            f"Подано пар          : {seen}\n"
            f"Додано нових        : {added}\n"
            f"Відслідковуємо всього: {total_pairs}"
        )

    try:
        await context.bot.edit_message_text(
            chat_id=context._chat_id, message_id=msg_id, text=text
        )
    except Exception:
        pass

async def _safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass

# ────────────────────────── setup() ──
def setup(app: Application) -> None:
    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Парсинг пар$"), parsing_start)],
        states={
            PARSE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, parsing_message),
                CallbackQueryHandler(parsing_finish, pattern="^finish$"),
                CallbackQueryHandler(parsing_cancel, pattern="^cancel$"),
            ]
        },
        fallbacks=[],
        per_message=False,
    )
    app.add_handler(conv)

    # команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("tick", manual_tick))
    app.add_handler(CommandHandler("sync_history", sync_history_cmd))
    app.add_handler(CommandHandler("status", status_cmd))

    # inline-callback для бек-філу
    app.add_handler(CallbackQueryHandler(refresh_callback, pattern="^refresh_"))
