import re
from typing import List, Tuple

SPACE_LIKE = "\u00a0\u200b\u200c\u200d\u2060\ufeff"

# 🔵 тепер ≥1 символ (було {2,})
TOKEN_RE = r"[A-Z0-9_\-]{1,}"

PAIR_RE = re.compile(fr"({TOKEN_RE})/({TOKEN_RE})", flags=re.I)

def extract_pairs(text: str) -> List[Tuple[str, str]]:
    if not text:
        return []

    # нормалізуємо NBSP, ZWSP → пробіл
    for ch in SPACE_LIKE:
        text = text.replace(ch, " ")

    pairs = [(m.group(1).upper(), m.group(2).upper()) for m in PAIR_RE.finditer(text)]
    return pairs
