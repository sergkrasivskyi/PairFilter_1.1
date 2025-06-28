"""
core/prices_ws.py  –  WebSocket-стрім усіх 15-хв. свічок.

Ключові зміни v2.2
──────────────────
• URL мульти-стріму будується від бази WS_ENDPOINT, *видаляючи* '/ws'
  (бо правильний шлях: …/stream?streams=).
• Перебудова пулу сокетів раз на 15 хв (900 с) перед плановим _tick.
• Лог кожної закритої свічки: INFO  WS BTCUSDT ts=…
"""

from __future__ import annotations

import asyncio, json, logging, time
from itertools import islice
from typing import Any, List, Set

import websockets

from settings import WS_ENDPOINT
from db.sqlite import DB
import core.prices as pr

log = logging.getLogger("pairfilter.ws")

MAX_STREAMS = 180               # <200 → гарантовано <4 kB URL
REFRESH_SEC = 900               # раз на 15 хв перевіряємо символи


# ───────── helpers ──────────────────────────────────────────
def chunks(seq: List[str], n: int):
    it = iter(seq)
    while True:
        block = list(islice(it, n))
        if not block:
            return
        yield block


def row_from_msg(msg: dict[str, Any]):
    if msg.get("e") != "kline":
        return None
    k = msg["k"]
    if not k.get("x"):          # незакрита свічка
        return None
    return k["s"], int(k["t"] // 1000), float(k["c"])


async def save_row(row, db: DB):
    if pr.history_refreshing:
        pr.pending_rows.append(row)
    else:
        await db.save_prices([row])
        log.info("WS %s ts=%s", row[0], row[1])


async def socket_worker(url: str, db: DB):
    """Один WebSocket з ≤180 streams."""
    while True:
        try:
            async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                log.info("WS open %s", url[-80:])
                async for raw in ws:
                    pkt = json.loads(raw)           # {"stream": "...", "data": {...}}
                    row = row_from_msg(pkt["data"])
                    if row:
                        await save_row(row, db)
        except Exception as e:
            log.error("WS err %s → reconnect 5 s", e)
            await asyncio.sleep(5)


# ───────── main loop ────────────────────────────────────────
async def start_listener(db: DB):
    """
    Піднімає пул WS-підключень, що накривають усі active-symbol@kline_15m.
    Раз на REFRESH_SEC перевіряє, чи змінився набір символів, і
    перезапускає пул, якщо так.
    """
    current: Set[str] = set()
    workers: List[asyncio.Task] = []

    # база URL без «/ws»
    base_ws = WS_ENDPOINT.replace("/ws", "")

    while True:
        symbols = {
            s async for a, b in db.iter_pairs()
            for s in (a, b)
        }
        if symbols != current:
            current = symbols
            # зупиняємо старі сокети
            for t in workers:
                t.cancel()
            workers.clear()

            for group in chunks(sorted(current), MAX_STREAMS):
                streams = "/".join(f"{sym.lower()}@kline_15m" for sym in group)
                url = f"{base_ws}/stream?streams={streams}"
                workers.append(asyncio.create_task(socket_worker(url, db)))

            log.info("WS-пул оновлено: %s symbols → %s sockets",
                     len(current), len(workers))

        await asyncio.sleep(REFRESH_SEC)
