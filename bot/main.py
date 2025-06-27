"""
main.py — точка входу Telegram-бота «PairFilter».

• Піднімає Application (python-telegram-bot v21).
• У post_init відкриває SQLite та запускає планувальник.
• Усі хендлери (start, Парсинг пар, /tick, /sync_history, /status,
  inline refresh_yes|no) додаються однією функцією bot.handlers.setup().
"""

from __future__ import annotations

import logging

from telegram.ext import Application

from settings import BOT_TOKEN
from logger import setup as log_setup
from db.sqlite import DB
from bot.scheduler import setup as sched_setup
from bot.handlers import setup as handlers_setup   # ← підключає ВСІ хендлери

log = logging.getLogger("pairfilter")


# ───────────────────────── post_init ─────────────────────────
async def _post_init(app: Application) -> None:
    """
    Викликається AUTOMATICALLY після build().
    • Підключаємо БД (aiosqlite)
    • Стартуємо планувальник (_tick кожні 0 / 15 / 30 / 45 хв)
    """
    db = DB()
    await db.connect()
    app.bot_data["db"] = db
    sched_setup(app)
    log.info("post_init завершено: БД + планувальник готові")


# ───────────────────────── main() ────────────────────────────
def main() -> None:
    """Конфігурує й запускає бот у режимі long-polling."""
    # 1️⃣ Логування: INFO у bot.log + stdout
    log_setup()

    # 2️⃣ Створюємо Application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_post_init)
        .build()
    )

    # 3️⃣ Реєструємо ВСІ команди / кнопки / callback-и
    handlers_setup(application)
    log.info("Handlers зареєстровані")

    # 4️⃣ Запускаємо long-polling
    application.run_polling(close_loop=False)


# ───────────────────────── entry-point ───────────────────────
if __name__ == "__main__":
    main()
