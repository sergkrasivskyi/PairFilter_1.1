"""
settings.py
------------
Усі змінні конфігурації PairFilter, що читаються з .env.
"""

from pathlib import Path
import os
from dotenv import load_dotenv

# ────────────────────────────────────────────────────────────
# 1.  Завантажуємо .env (лежить у корені репозиторію)
# ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ────────────────────────────────────────────────────────────
# 2.  Telegram-бот
# ────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.getenv("BOT_TOKEN")                      # обов’язково
WORK_CHAT_ID: int = int(os.getenv("WORK_CHAT_ID", "0"))      # чат розробки
ALERT_CHANNEL_ID: int = int(os.getenv("ALERT_CHANNEL_ID", "0"))

# Куди слати інлайн-запит «Оновити історію?».
# Якщо у .env явно не вказано ― використовуємо WORK_CHAT_ID.
FULL_REFRESH_CONFIRM_CHAT_ID: int = int(
    os.getenv("FULL_REFRESH_CONFIRM_CHAT_ID", WORK_CHAT_ID)
)

# ────────────────────────────────────────────────────────────
# 3.  Зовнішні сервіси
# ────────────────────────────────────────────────────────────
BINANCE_URL: str = os.getenv("BINANCE_URL", "https://api.binance.com")
WS_ENDPOINT: str = os.getenv(
    "WS_ENDPOINT", "wss://stream.binance.com:9443"
)

# ────────────────────────────────────────────────────────────
# 4.  База даних
# ────────────────────────────────────────────────────────────
DB_PATH: Path = BASE_DIR / "pairfilter.db"

# ────────────────────────────────────────────────────────────
# 5.  Робочі константи PairFilter
# ────────────────────────────────────────────────────────────
HIST_LEN: int = int(os.getenv("HIST_LEN", 672))          # 7 днів * 96
Z_THRESHOLD: float = float(os.getenv("Z_THRESHOLD", 3.0))

# «Свіжа» свічка — різниця ts менша за HISTORY_FRESH_SEC
HISTORY_FRESH_SEC: int = int(os.getenv("HISTORY_FRESH_SEC", 20 * 60))

# HTTP-timeout, використовується у сцені «Парсинг пар»
TIMEOUT_SEC: int = 30
# True → REST + WebSocet; False → тільки WebSocket
REST_SYNC_ENABLED = False        