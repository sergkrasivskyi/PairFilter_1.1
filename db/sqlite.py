"""
db/sqlite.py
────────────
Асинхронний доступ до SQLite із WAL-режимом і глобальним DB_LOCK.

Нове у 1.1.2
• Якщо стара база вже містить watched_pairs без поля `inactive`,
  connect() автоматично виконує
      ALTER TABLE watched_pairs ADD COLUMN inactive INTEGER NOT NULL DEFAULT 0
  і пише про це у лог.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import aiosqlite

from settings import DB_PATH

log = logging.getLogger("pairfilter")

# ────────────────────────────────────────────────────────────
# DDL
# ────────────────────────────────────────────────────────────
CREATE_PAIRS = """
CREATE TABLE IF NOT EXISTS watched_pairs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sym_a    TEXT NOT NULL,
    sym_b    TEXT NOT NULL,
    added_ts INTEGER NOT NULL,
    inactive INTEGER NOT NULL DEFAULT 0,
    UNIQUE (sym_a, sym_b)
);
"""

CREATE_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT NOT NULL,
    ts     INTEGER NOT NULL,
    close  REAL    NOT NULL,
    PRIMARY KEY (symbol, ts)
);
"""

# ────────────────────────────────────────────────────────────
# Глобальний lock на запис
# ────────────────────────────────────────────────────────────
DB_LOCK = asyncio.Lock()


class DB:
    """Singleton-обгортка над одним з’єднанням aiosqlite."""

    def __init__(self, path: Path = DB_PATH):
        self._path = path
        self.conn: aiosqlite.Connection | None = None

    # ────────────────────────── init ──
    async def connect(self) -> None:
        """
        Відкриває одне з’єднання й вмикає WAL + busy_timeout.
        Якщо таблиця watched_pairs старого формату — додає стовпець inactive.
        """
        if self.conn:
            return

        self.conn = await aiosqlite.connect(
            self._path, timeout=5, isolation_level=None
        )
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA busy_timeout=3000")

        # створюємо таблиці, якщо їх немає
        await self.conn.execute(CREATE_PAIRS)
        await self.conn.execute(CREATE_PRICES)

        # ── міграція: перевіряємо, чи є стовпець inactive ──
        async with self.conn.execute("PRAGMA table_info(watched_pairs)") as cur:
            col_names = [row[1] async for row in cur]   # row[1] = name
        if "inactive" not in col_names:
            await self.conn.execute(
                "ALTER TABLE watched_pairs "
                "ADD COLUMN inactive INTEGER NOT NULL DEFAULT 0"
            )
            log.info("DB-migration: додано стовпець 'inactive' до watched_pairs")

        await self.conn.commit()

    async def _ensure_conn(self) -> None:
        if self.conn is None:
            await self.connect()

    # ───────────────── watched_pairs API ──
    async def add_pair(self, sym_a: str, sym_b: str) -> bool:
        """
                Додає (INSERT) або ре-активує (UPDATE inactive→0) пару.
        
                Повертає:
                    True   – пара була відсутня у БД, вставлено новий рядок;
                    False  – пара існувала (active чи inactive) й лише
                             оновлено inactive=0 або взагалі нічого не змінено.
                """
        await self._ensure_conn()
        async with DB_LOCK:
            inserted = False
            try:
                await self.conn.execute(
                    """
                    INSERT INTO watched_pairs(sym_a, sym_b, added_ts)
                    VALUES(?, ?, strftime('%s','now'))
                    """,
                    (sym_a, sym_b),
                )
                inserted = True
            except aiosqlite.IntegrityError:
                # пара є → робимо active=0 → active=1
                await self.conn.execute(
                    """
                    UPDATE watched_pairs
                    SET inactive = 0
                    WHERE sym_a=? AND sym_b=?
                    """,
                    (sym_a, sym_b),
                )
            await self.conn.commit()
        return inserted

    async def iter_pairs(self):
        """Асинхронний генератор активних пар (sym_a, sym_b)."""
        await self._ensure_conn()
        async with self.conn.execute(
            "SELECT sym_a, sym_b FROM watched_pairs WHERE inactive=0"
        ) as cur:
            async for row in cur:
                yield row

    # ───────────────── inactive helpers ──
    async def mark_inactive(self, symbol: str) -> None:
        """
        Позначає ВСІ пари, де symbol зустрічається, як inactive=1.
        Викликається, коли біржа повертає -1121 / порожній масив.
        """
        await self._ensure_conn()
        async with DB_LOCK:
            await self.conn.execute(
                """
                UPDATE watched_pairs
                SET inactive = 1
                WHERE sym_a = ? OR sym_b = ?
                """,
                (symbol, symbol),
            )
            await self.conn.commit()

    async def is_inactive(self, symbol: str) -> bool:
        """True, якщо symbol входить у хоча б одну пару inactive=1."""
        await self._ensure_conn()
        async with self.conn.execute(
            """
            SELECT 1 FROM watched_pairs
            WHERE inactive=1 AND (sym_a=? OR sym_b=?) LIMIT 1
            """,
            (symbol, symbol),
        ) as cur:
            return await cur.fetchone() is not None

    # ───────────────── prices API ──
    async def save_prices(self, rows: list[tuple[str, int, float]]) -> None:
        """Масове `INSERT OR IGNORE` (відразу коміт)."""
        if not rows:
            return
        await self._ensure_conn()
        async with DB_LOCK:
            await self.conn.executemany(
                "INSERT OR IGNORE INTO prices(symbol, ts, close) VALUES(?,?,?)",
                rows,
            )
            await self.conn.commit()

    async def get_series(self, symbol: str, limit: int):
        """list[(ts, close)] max=`limit`, у порядку старі → нові."""
        await self._ensure_conn()
        async with self.conn.execute(
            """
            SELECT ts, close FROM prices
            WHERE symbol = ? ORDER BY ts DESC LIMIT ?
            """,
            (symbol, limit),
        ) as cur:
            data = await cur.fetchall()
        return list(reversed(data))
