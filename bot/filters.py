# bot/filters.py
import re
from telegram import Message
from telegram.ext.filters import MessageFilter

# пробілоподібні символи, які треба визнати «space»
SPACE_LIKE = "\u00a0\u200b\u200c\u200d\u2060\ufeff"

class IsParseCmd(MessageFilter):
    def filter(self, message: Message) -> bool:  # noqa: D401
        txt = message.text
        if not txt:
            return False

        txt = txt.lower()
        for ch in SPACE_LIKE:
            txt = txt.replace(ch, " ")

        txt = re.sub(r"[^\w\s]", "", txt)     # прибираємо emoji, пунктуацію
        txt = re.sub(r"\s+", " ", txt).strip()

        return txt == "парсинг пар"

parse_filter = IsParseCmd()
