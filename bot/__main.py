"""
main.py — точка входу Telegram-бота «PairFilter».

• Піднімає Application (python-telegram-bot v21).
• У post_init відкриває SQLite та запускає планувальник (0 / 15 / 30 / 45 хв).
• Реєструє хендлери:
    /start
    сцена «Парсинг пар»  (✅ Завершити / ❌ Скасувати)
    кнопка «Оновити ціни»
    «меню за замовчуванням» («Виберіть опцію»)
"""

from __future__ import annotations
import logging

from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from settings import BOT_TOKEN
from logger import setup as log_setup
from db.sqlite import DB
from bot.filters import parse_filter                  # кастомний фільтр «Парсинг пар»
from bot.handlers import (                            # callback-функції
    start,
    parsing_start,
    parsing_message,
    parsing_finish,                                   # ✅
    parsing_cancel,                                   # ❌
    manual_tick,                                      # «Оновити ціни»
    menu_keyboard,
    PARSE,
)
from bot.scheduler import setup as sched_setup


# ───────────────────────────── post_init ──
async def _post_init(app: Application) -> None:
    """
    Викликається AUTOMATICALLY усередині event-loop одразу після build().
    • Підключаємо БД (aiosqlite)
    • Стартуємо планувальник (_tick кожні 0 / 15 / 30 / 45 хв)
    """
    db = DB()
    await db.connect()
    app.bot_data["db"] = db
    sched_setup(app)


# ───────────────────────────── main() ──
def main() -> None:
    """Конфігурує й запускає бот у режимі long-polling."""
    # 1️⃣ Логування: INFO у bot.log + stdout
    log_setup()

    # 2️⃣ Створюємо Application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_post_init)               # ← БД + планувальник
        .build()
    )

    # 3️⃣  Conversation «Парсинг пар»
    #    entry_points —  лиш один фільтр parse_filter
    conv = ConversationHandler(
        entry_points=[MessageHandler(parse_filter, parsing_start)],
        states={
            PARSE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, parsing_message),
                CallbackQueryHandler(parsing_finish, pattern="^finish$"),
                CallbackQueryHandler(parsing_cancel, pattern="^cancel$"),
            ]
        },
        fallbacks=[],
        allow_reentry=True,
        # per_message=True,                    # усуває PTBUserWarning
    )
    application.add_handler(conv)            # 👉 ДОДАЄМО ПЕРШИМ!

    # 4️⃣ /start → головне меню
    application.add_handler(CommandHandler("start", start))

    # 5️⃣ Кнопка «Оновити ціни» (нечутливо до регістру / пробілів)
    update_flt = filters.Regex(r"(?i)^\s*оновити\s+ціни\s*$")
    application.add_handler(MessageHandler(update_flt, manual_tick))

    # 6️⃣ Меню за замовчуванням (усі інші текстові повідомлення)
    menu_flt = filters.TEXT & ~filters.COMMAND & ~parse_filter & ~update_flt
    application.add_handler(
        MessageHandler(
            menu_flt,
            lambda u, c: u.message.reply_text(
                "Виберіть опцію:", reply_markup=menu_keyboard()
            ),
        )
    )

    # 7️⃣ Запускаємо long-polling (створює event-loop сам)
    application.run_polling(close_loop=False)


# ───────────────────────────── entry-point ──
if __name__ == "__main__":
    main()
