"""
core/prices.py
────────────────
REST-шар роботи з цінами Binance.

Нове у 1.2
• GLOBAL  history_refreshing  — True під час масового бек-філу.
• Функція backfill(symbols)   — одноразово тягне HIST_LEN свічок.
• _symbol_stats()             — правильне missing = max(gap_tail, gap_len)
• Інші модулі (WS-слухач, scheduler) читають/змінюють прапорець напряму.
"""

from __future__ import annotations

import asyncio
import logging
import time
from time import perf_counter
from typing import List, Tuple

import httpx

from settings import BINANCE_URL, HIST_LEN
from db.sqlite import DB

# ────────────────────────────────────────────────────────────
# Глобальний прапорець «йде масовий бек-філ»
# ────────────────────────────────────────────────────────────
history_refreshing: bool = False

# буфер для WS-свічок (core/prices_ws.py його зіллє)
pending_rows: list[tuple[str, int, float]] = []

log = logging.getLogger("pairfilter")

# ────────────────────────────────────────────────────────────
# Low-level fetch
# ────────────────────────────────────────────────────────────
async def _fetch_klines(symbol: str, *, limit: int, interval: str = "15m"):
    url = f"{BINANCE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    backoff = 1
    for _ in range(6):
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(url, params=params)
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, httpx.ConnectError) as e:
            log.warning("Binance %s для %s — retry %s s", e, symbol, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    raise RuntimeError(f"Binance API unavailable для {symbol}")

# ────────────────────────────────────────────────────────────
# 1. Функції для головного планувальника
# ────────────────────────────────────────────────────────────
async def _symbol_stats(db: DB, symbol: str) -> Tuple[int, int]:
    """
    Повертає (have, missing):
      have     – рядків у БД;
      missing  – скільки треба довантажити (0…HIST_LEN).
    Формула: missing = max( gap у хвості , HIST_LEN-have )
    """
    await db.connect()
    async with db.conn.execute(
        "SELECT COUNT(*), MAX(ts) FROM prices WHERE symbol=?", (symbol,)
    ) as cur:
        cnt, last_ts = await cur.fetchone()

    cnt = int(cnt)
    now_aligned = int(time.time() // 900) * 900

    # розрив у хвості
    if last_ts is None:
        gap_tail = HIST_LEN
    else:
        gap_raw = (now_aligned - last_ts) // 900
        gap_tail = max(0, min(gap_raw, HIST_LEN))

    gap_len = max(0, HIST_LEN - cnt)

    missing = max(gap_tail, gap_len)
    return cnt, missing


async def sync_prices_for_pairs(db: DB, *, concurrency: int = 8) -> None:
    """
    • Пропускає роботу, якщо зараз іде history_refreshing.
    • Інакше довантажує тільки missing для кожного symbol.
    """
    if history_refreshing:
        log.info("price-sync пропущено (йде масовий бек-філ)")
        return

    await db.connect()

    pair_rows = [row async for row in db.iter_pairs()]
    symbols: set[str] = {s for a, b in pair_rows for s in (a, b)}
    log.info("зчитано %s пар, унікальних токенів %s", len(pair_rows), len(symbols))

    sem = asyncio.Semaphore(concurrency)
    t0 = perf_counter()

    async def _worker(sym: str):
        async with sem:
            have, missing = await _symbol_stats(db, sym)
            if missing == 0:
                log.info("%s — у БД %s, бракує 0", sym, have)
                return
            klines = await _fetch_klines(sym, limit=missing)
            if not klines:
                await db.mark_inactive(sym)
                log.warning("%s – порожня відповідь, позначено inactive", sym)
                return
            rows = [(sym, int(k[0] // 1000), float(k[4])) for k in klines]
            await db.save_prices(rows)
            log.info("%s — у БД %s, бракує %s, завантажено %s", sym, have, missing, len(rows))

    await asyncio.gather(*(_worker(s) for s in symbols))
    log.info("price-sync завершено (%s s)", perf_counter() - t0)

# ────────────────────────────────────────────────────────────
# 2. Масовий бек-філ (викликається після підтвердження)
# ────────────────────────────────────────────────────────────
async def backfill(db: DB, symbols: list[str]) -> None:
    """
    Ставить history_refreshing=True, тягне HIST_LEN свічок
    для кожного symbol, потім скидає прапорець і зливає WS-буфер.
    """
    global history_refreshing, pending_rows
    history_refreshing = True
    log.info("⇢ почався масовий бек-філ (%s токенів)", len(symbols))

    sem = asyncio.Semaphore(8)

    async def _worker(sym: str):
        async with sem:
            klines = await _fetch_klines(sym, limit=HIST_LEN)
            rows = [(sym, int(k[0] // 1000), float(k[4])) for k in klines]
            await db.save_prices(rows)
            log.info("%s — повний бек-філ (%s рядків)", sym, len(rows))

    await asyncio.gather(*(_worker(s) for s in symbols))

    # вставляємо WS-буфер, якщо щось накопичилось
    if pending_rows:
        await db.save_prices(pending_rows)
        log.info("злитий WS-буфер: %s рядків", len(pending_rows))
        pending_rows = []

    history_refreshing = False
    log.info("⇠ масовий бек-філ завершено")

# ────────────────────────────────────────────────────────────
# 3. Сумісність зі старим кодом
# ────────────────────────────────────────────────────────────
async def latest_close(symbol: str) -> Tuple[int, float]:
    """(ts, close) останньої закритої 15-хв. свічки."""
    k = await _fetch_klines(symbol, limit=1)
    ts_ms, _, _, _, close, _ = k[0][:6]
    return int(ts_ms // 1000), float(close)
