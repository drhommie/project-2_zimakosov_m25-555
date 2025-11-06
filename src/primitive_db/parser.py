# src/primitive_db/parser.py

from __future__ import annotations

import shlex
import re
from typing import Any, Dict, List, Tuple

# ---------- КОНСТАНТЫ ----------
OP_EQ = "="
SEP_COMMA = ","
SEP_AND = "and"

TRUE_TOKENS = {"true", "1", "yes", "y"}
FALSE_TOKENS = {"false", "0", "no", "n"}

ERR_EMPTY = "Пустое выражение."
ERR_EXPECT_KV = 'Ожидался шаблон вида: <колонка> = <значение>. Получено: "{}"'
ERR_UNKNOWN_SEPARATOR = 'Некорректный разделитель между парами. Используйте "," для SET или "and" для WHERE. Ошибка около: "{}"'
ERR_EXPECT_VALUE = 'Ожидалось значение после "=" около: "{}"'
ERR_DUPLICATE_KEYS = 'Дублируется колонка "{}"'
ERR_EMPTY_KEY = "Пустое имя колонки."


# ---------- ДЕКОРАТОР ЕДИНООБРАЗНЫХ ОШИБОК ----------

def _parse_guard(fn):
    def wrapper(text: str) -> Dict[str, Any] | List[Any]:
        try:
            return fn(text)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Некорректное выражение: {e}")
    return wrapper


# ---------- ПРИВЕДЕНИЕ ТИПОВ ----------

def _infer_scalar(token: str) -> Any:
    """
    - true/false/yes/no/1/0 -> bool
    - целые числа -> int
    - остальное -> str
    Требование: строковые значения в командах в кавычках.
    """
    # Чистим «мусорные» символы между буквами (fa?lse, tru​e и т.п.)
    token = re.sub(r"[^A-Za-z0-9_+\-]", "", token)

    low = token.lower()
    if low in TRUE_TOKENS:
        return True
    if low in FALSE_TOKENS:
        return False
    # int?
    if (low.startswith(("+", "-")) and low[1:].isdigit()) or low.isdigit():
        try:
            return int(low)
        except Exception:
            pass
    return token  # строка (шlex уже снял кавычки)


# ---------- ВСПОМОГАТЕЛЬНЫЙ РАЗБОР ПАР key=value ----------

def _split_assignments(tokens: List[str], allowed_sep: str) -> List[Tuple[str, str]]:
    """
    Разбирает поток токенов в пары (key, value) c поддержкой:
      SET:  key = val , key2 = val2 , ...
      WHERE: key = val and key2 = val2 and ...
    allowed_sep: SEP_COMMA (для SET) или SEP_AND (для WHERE)
    """
    pairs: List[Tuple[str, str]] = []
    i = 0
    n = len(tokens)
    while i < n:
        key = tokens[i] if i < n else ""
        if not key:
            raise ValueError(ERR_EMPTY_KEY)
        if i + 1 >= n or tokens[i + 1] != OP_EQ:
            frag = " ".join(tokens[i:i+3])
            raise ValueError(ERR_EXPECT_KV.format(frag))
        if i + 2 >= n:
            frag = " ".join(tokens[max(0, i-1):i+3])
            raise ValueError(ERR_EXPECT_VALUE.format(frag))

        val = tokens[i + 2]
        pairs.append((key, val))
        i += 3

        if i < n:
            sep = tokens[i].lower()
            if sep != allowed_sep:
                frag = " ".join(tokens[max(0, i-2):i+3])
                raise ValueError(ERR_UNKNOWN_SEPARATOR.format(frag))
            i += 1
    return pairs


# ---------- ПУБЛИЧНЫЕ ПАРСЕРЫ ----------

@_parse_guard
def parse_set(text: str) -> Dict[str, Any]:
    """
    Пример:
      'age = 29, is_active = true, title = "The Hobbit"'
      -> {'age': 29, 'is_active': True, 'title': 'The Hobbit'}
    """
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    normalized = text.replace(",", f" {SEP_COMMA} ")
    tokens = shlex.split(normalized, posix=True)

    # унифицируем «разделитель» — заменим запятую на ключевое слово
    tokens = [SEP_AND if t == SEP_COMMA else t for t in tokens]
    pairs = _split_assignments(tokens, allowed_sep=SEP_AND)

    out: Dict[str, Any] = {}
    for k, raw in pairs:
        if not k:
            raise ValueError(ERR_EMPTY_KEY)
        if k in out:
            raise ValueError(ERR_DUPLICATE_KEYS.format(k))
        out[k] = _infer_scalar(raw)
    return out


@_parse_guard
def parse_where(text: str) -> Dict[str, Any]:
    """
    Пример:
      'year = 1937 and title = "The Hobbit" and is_available = true'
      -> {'year': 1937, 'title': 'The Hobbit', 'is_available': True}
    """
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    tokens = shlex.split(text, posix=True)
    pairs = _split_assignments(tokens, allowed_sep=SEP_AND)

    out: Dict[str, Any] = {}
    for k, raw in pairs:
        if not k:
            raise ValueError(ERR_EMPTY_KEY)
        if k in out:
            raise ValueError(ERR_DUPLICATE_KEYS.format(k))
        out[k] = _infer_scalar(raw)
    return out


@_parse_guard
def parse_values_list(text: str) -> List[Any]:
    """
    Пример:
      '"The Hobbit", "Tolkien", 1937, true'
      -> ['The Hobbit', 'Tolkien', 1937, True]
    Упрощение: строки в кавычках.
    """
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    # снимаем внешние скобки, если пришли вместе с values(...)
    text = text.strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()

    tokens = shlex.split(text, posix=True)
    clean_tokens = [t for t in tokens if t != "," and t != "，"]
    return [_infer_scalar(t) for t in clean_tokens]
