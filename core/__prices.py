"""
core/prices.py
────────────────
Збирання 15-хвилинних свічок з Binance для ВСІХ токенів із таблиці
watched_pairs.

Алгоритм:
1. Збираємо унікальні symbols прямо з БД (вони вже мають суфікс USDT).
2. Для кожного symbol-а дивимось, скільки свічок бракує до поточного
   закритого інтервалу:
      • missing == 0           → нічого не тягнемо;
      • 1 ≤ missing < HIST_LEN → робимо один запит limit = missing;
      • missing ≥ HIST_LEN
        або symbol взагалі відсутній у prices
        → тягнемо одразу HIST_LEN (672) свічок.
3. Записуємо результат через DB.save_prices(), який уже захищено WAL-ом
   і глобальним asyncio.Lock усередині db.sqlite.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Tuple

import httpx

from settings import BINANCE_URL, HIST_LEN
from db.sqlite import DB

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# Низькорівневий REST-виклик до Binance з експоненційним back-off
# ────────────────────────────────────────────────────────────
async def _fetch_klines(symbol: str, *, limit: int, interval: str = "15m"):
    """
    Повертає JSON-масив свічок у форматі, який віддає Binance.
    limit — від 1 до 1000 (обмеження біржі).
    """
    url = f"{BINANCE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    backoff = 1
    for _ in range(6):  # 1 + 2 + 4 + 8 + 16 + 32 = 63 с
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(url, params=params)
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, httpx.ConnectError) as e:
            log.warning("Binance %s → retry in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    raise RuntimeError("Binance API unavailable")


# ────────────────────────────────────────────────────────────
# Сумісність зі старими викликами (повертає останню закриту свічку)
# ────────────────────────────────────────────────────────────
async def latest_close(symbol: str) -> Tuple[int, float]:
    """
    Повертає (timestamp_sec, close) для останньої 15-хв. свічки.
    Використовується старим кодом; новий планувальник працює через
    sync_prices_for_pairs().
    """
    klines = await _fetch_klines(symbol, limit=1)
    ts_ms, _, _, _, close, _ = klines[0][:6]
    return int(ts_ms // 1000), float(close)


# ────────────────────────────────────────────────────────────
# Скільки свічок бракує до останнього закритого інтервалу?
# ────────────────────────────────────────────────────────────
async def _missing_count(db: DB, symbol: str) -> int:
    """
    0          → оновлення не потрібне;
    1…671      → саме така кількість відсутніх свічок;
    ≥ HIST_LEN → бракує 672 або більше, робимо повний бекфіл.
    """
    await db.connect()                     # гарантуємо наявність conn
    async with db.conn.execute(
        "SELECT MAX(ts) FROM prices WHERE symbol = ?", (symbol,)
    ) as cur:
        last_row = await cur.fetchone()

    last_ts = last_row[0]                  # може бути None
    now_aligned = int(time.time() // 900) * 900  # останнє закрите «15m»

    if last_ts is None:
        return HIST_LEN

    missing = (now_aligned - last_ts) // 900
    if missing <= 0:
        return 0
    return missing if missing < HIST_LEN else HIST_LEN


# ────────────────────────────────────────────────────────────
# Головна функція: повна синхронізація всіх токенів
# ────────────────────────────────────────────────────────────
async def sync_prices_for_pairs(db: DB, *, concurrency: int = 6) -> None:
    """
    • Викликається планувальником (00/15/30/45) або вручну при старті.
    • Після виконання у БД гарантовано буде безперервний ряд із щонайменше
      HIST_LEN свічок для кожного symbol-а з watched_pairs.
    """
    await db.connect()

    # 1️⃣ Унікальний список токенів (вони вже з USDT)
    symbols: set[str] = set()
    async for a, b in db.iter_pairs():
        symbols.update({a, b})

    if not symbols:
        log.warning("watched_pairs порожній — нічого синхронізувати")
        return

    # 2️⃣ Паралельний збір із семафором, щоби не навантажити REST-ліміти
    sem = asyncio.Semaphore(concurrency)

    async def _worker(sym: str):
        async with sem:
            miss = await _missing_count(db, sym)
            if miss == 0:
                return                                      # усе актуально
            klines = await _fetch_klines(sym, limit=miss)
            rows: List[Tuple[str, int, float]] = [
                (sym, int(k[0] // 1000), float(k[4])) for k in klines
            ]
            await db.save_prices(rows)
            log.info("%s — додано %s свічок", sym, len(rows))

    await asyncio.gather(*(_worker(s) for s in symbols))
    log.info("✅ sync_prices_for_pairs: опрацьовано %s symbols", len(symbols))
