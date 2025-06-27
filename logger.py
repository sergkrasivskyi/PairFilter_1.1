"""
logger.py
──────────
Єдине місце, де конфігурується логування PairFilter-бота.

• Формат: 2025-06-24 16:10:34,123 [INFO] pairfilter: повідомлення
• Два хендлери: файл «bot.log» + консоль (stdout)
• Шум від httpx та python-telegram-bot переводимо на рівень WARNING,
  щоби в логах не з’являлися рядки типу
  «HTTP Request: POST ... getUpdates "200 OK"».
"""

from __future__ import annotations

import logging
import sys


def setup() -> None:
    """Викликається один раз у main.py до імпорту будь-яких наших модулів."""
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler("bot.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,          # перекриваємо можливі попередні конфігурації
    )

    # ── приглушаємо сторонні бібліотеки ───────────────────────
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

    # опційно: якщо виникне інший «балакучий» саблогер,
    # додайте його сюди аналогічним рядком:
    # logging.getLogger("some_library").setLevel(logging.WARNING)

    logging.getLogger("pairfilter").info("Логер ініціалізовано")
