"""
bot/scheduler.py
────────────────
Планувальник PairFilter 1.1 (+).

Алгоритм оновлення цін
──────────────────────
1.  WebSocket-слухач (!)   → основне джерело.  Кожні 15 хв, коли Binance
    надсилає закриту свічку, ми одразу робимо
        INSERT OR IGNORE (symbol, ts, close)
    у таблицю prices.

2.  Плановий REST-крон (0 / 15 / 30 / 45 UTC)  → резерв:
        • якщо history_refreshing = True  → пропускаємо;
        • інакше довантажуємо лише missing_tail (0…2 свічки).
    Це страхує випадки, коли WS-стрім обірвався на кілька хвилин.

3.  Cold-start / нові токени  → масовий бек-філ 672×n свічок:
        • бот оцінює повноту історії одразу після запуску;
        • якщо є нестача → питає «Так / Ні» у робочому чаті;
        • під прапорцем history_refreshing бек-філ зливає WS-буфер
          після завершення і далі повертає керування WS + REST.

Формат логів збережений: INFO-рядки для кожної свічки/пари, DEBUG -
для допоміжних подій.
"""

from __future__ import annotations

import asyncio
import logging
from time import perf_counter
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from settings import (
    ALERT_CHANNEL_ID,
    FULL_REFRESH_CONFIRM_CHAT_ID,
    HIST_LEN,
    HISTORY_FRESH_SEC,
    Z_THRESHOLD,
    REST_SYNC_ENABLED,
)
from db.sqlite import DB
from core import prices as pr
from core.prices import sync_prices_for_pairs, backfill
from core.prices_ws import start_listener
from core.zscore import compute

if TYPE_CHECKING:
    from telegram.ext import Application

log = logging.getLogger("pairfilter")

# ───────────────────────── utils ────────────────────────────
async def check_history(db: DB) -> list[str]:
    """
    Повертає символи, для яких:
      • COUNT(*) < HIST_LEN  або
      • now - max_ts > HISTORY_FRESH_SEC
    """
    await db.connect()
    async with db.conn.execute(
        "SELECT symbol, COUNT(*), MAX(ts) FROM prices GROUP BY symbol"
    ) as cur:
        rows = await cur.fetchall()

    now_ts = int(perf_counter())
    need: list[str] = []
    for sym, cnt, last_ts in rows:
        cnt = int(cnt)
        if cnt < HIST_LEN or (now_ts - last_ts) > HISTORY_FRESH_SEC:
            need.append(sym)
    return need


async def ask_full_refresh(app: "Application", symbols: list[str]) -> None:
    """Інлайн-запит «Оновити історію?» у FULL_REFRESH_CONFIRM_CHAT_ID."""
    app.bot_data["refresh_tokens"] = symbols
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Так", callback_data="refresh_yes"),
          InlineKeyboardButton("Ні",  callback_data="refresh_no")]]
    )
    await app.bot.send_message(
        FULL_REFRESH_CONFIRM_CHAT_ID,
        f"🟡 Історія цін не повна або застаріла "
        f"({len(symbols)} токенів). Оновити 672×n свічок?",
        reply_markup=kb,
    )
    log.info("запит на бек-філ відправлено (%s символів)", len(symbols))

# ───────────────────────── setup ────────────────────────────
def setup(app: "Application") -> None:
    """Стартується з main.py – WS, первинний чек, cron-job."""
    # 1) WebSocket-listener (основне джерело свічок)
    app.create_task(start_listener(app.bot_data["db"]))

    # 2) первинна перевірка історії
    app.create_task(_initial_history_check(app))

    # 3) крон-job на 0/15/30/45
    sched = AsyncIOScheduler(timezone="UTC")
    sched.add_job(
        _tick,
        trigger="cron",
        minute="0,15,30,45",
        id="pairfilter_tick",
        kwargs={"app": app},
        replace_existing=True,
    )
    sched.start()
    log.info("Scheduler запущено (0/15/30/45 хв)")


async def _initial_history_check(app: "Application"):
    db: DB = app.bot_data["db"]
    symbols = await check_history(db)
    if symbols:
        await ask_full_refresh(app, symbols)

# ───────────────────────── tick ─────────────────────────────
async def _tick(
    app: "Application",
    *,
    manual_user: str | None = None,
    silent: bool = False,
) -> None:
    if manual_user:
        log.info("manual-tick запущено %s", manual_user)

    db: DB = app.bot_data["db"]

    # 1) довантажуємо хвости, якщо не йде бек-філ
    if REST_SYNC_ENABLED:
        await sync_prices_for_pairs(db)
    else:
        log.debug("REST-sync вимкнено – покладаємось на WebSocket")
    # 2) рахуємо Z-score
    t0 = perf_counter()
    pairs = [(a, b) async for a, b in db.iter_pairs()]
    signals = []

    for sym_a, sym_b in pairs:
        a_ser = await db.get_series(sym_a, HIST_LEN)
        b_ser = await db.get_series(sym_b, HIST_LEN)
        if len(a_ser) < HIST_LEN or len(b_ser) < HIST_LEN:
            log.info(
                "z-calc  %s/%s  skip (history %s/%s)",
                sym_a,
                sym_b,
                len(a_ser),
                len(b_ser),
            )
            continue

        _, a_px = zip(*a_ser)
        _, b_px = zip(*b_ser)
        sig = compute(list(a_px), list(b_px), sym_a, sym_b)
        if sig is None:
            continue

        log.info("z-calc  %s  z=%.2f", sig.pair, sig.z)
        if sig.z >= Z_THRESHOLD:
            signals.append(sig)

    log.info(
        "z-score: переглянуто %s, сигналів %s (%.2f s)",
        len(pairs),
        len(signals),
        perf_counter() - t0,
    )

    if not signals or silent:
        return

    # ── Формуємо та надсилаємо повідомлення ─────────────────
    signals.sort(key=lambda s: -s.z)           # TOP-список

    def strip_usdt(pair: str) -> str:
        return pair.replace("USDT", "")

    text_lines = [
        "🚨 TOP Z-Score",
        "ZSCR | PAIR",
        *[f"{s.z:.1f} | {strip_usdt(s.pair)}" for s in signals],
    ]
    await app.bot.send_message(ALERT_CHANNEL_ID, "\n".join(text_lines))
    log.info("надіслано %s сигнал(и) у канал %s", len(signals), ALERT_CHANNEL_ID)
