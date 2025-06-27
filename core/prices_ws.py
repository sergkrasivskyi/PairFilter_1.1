"""
core/prices_ws.py
──────────────────
WebSocket-слухач Binance, який пише 15-хв. свічки.

• При history_refreshing == True  — свічки складаються у
  core.prices.pending_rows і не лізуть у БД одразу.
• При звичайному стані — одразу INSERT OR IGNORE у БД.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import websockets

from settings import WS_ENDPOINT
from db.sqlite import DB
import core.prices as pr  # для history_refreshing та pending_rows

log = logging.getLogger("pairfilter.ws")

STREAM = "!kline_15m@arr"  # агрегований потік усіх 15-хв. свічок

async def _handle_msg(msg: dict[str, Any], db: DB) -> None:
    """Обробляє одне kline-повідомлення."""
    if msg.get("e") != "kline":
        return
    k = msg["k"]
    if not k.get("x"):           # x=False → свічка ще не закрита
        return

    symbol = k["s"]
    ts_sec = int(k["t"] // 1000)
    close = float(k["c"])
    row = (symbol, ts_sec, close)

    if pr.history_refreshing:
        pr.pending_rows.append(row)
    else:
        await db.save_prices([row])

async def start_listener(db: DB) -> None:
    """Запускається один раз з scheduler.setup(). Не завершується."""
    url = f"{WS_ENDPOINT}/{STREAM}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                log.info("WS-підключення відкрите")
                async for raw in ws:
                    msg = json.loads(raw)
                    await _handle_msg(msg, db)
        except Exception as e:
            log.error("WS-помилка %s — реконект через 5 с", e)
            await asyncio.sleep(5)
