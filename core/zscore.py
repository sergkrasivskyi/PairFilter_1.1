"""
core/zscore.py
────────────────
Розрахунок Z-score для крос-курсу двох активів (A/B).

Вхід:
    • a_px, b_px  – списки float однакової довжини (ціни «close»);
    • sym_a, sym_b – тікери A та B (з USDT або без – неважливо).

Алгоритм:
    1. cross_i = a_px_i / b_px_i
    2. μ, σ  – середнє та стандартне відхилення cross на всьому вікні
    3. z     = (cross_last – μ) / σ
    4. якщо z < 0, міняємо пару місцями й робимо z = |z|
       (усі сигнали «одного напряму» для спрощення фільтрації)

Повертає ZSignal або None, якщо std == 0 або різні довжини масивів.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np


@dataclass
class ZSignal:
    pair: str     # «TOKENA/TOKENB» (вже з нормалізованим напрямом)
    z: float      # абсолютне значення ≥ 0
    cross: float  # останній крос-курс
    mean: float
    std: float


def compute(
    a_px: List[float],
    b_px: List[float],
    sym_a: str,
    sym_b: str,
) -> Optional[ZSignal]:
    """Повертає ZSignal або None, якщо std==0 чи довжини не збігаються."""
    if len(a_px) != len(b_px) or not a_px:
        return None

    cross = np.array(a_px) / np.array(b_px)
    mean = cross.mean()
    std = cross.std(ddof=1)
    if std == 0:
        return None

    z = (cross[-1] - mean) / std

    # нормалізуємо знак
    if z < 0:
        z = abs(z)
        pair_name = f"{sym_b}/{sym_a}"
    else:
        pair_name = f"{sym_a}/{sym_b}"

    return ZSignal(pair=pair_name, z=z, cross=float(cross[-1]), mean=float(mean), std=float(std))
