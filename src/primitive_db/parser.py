# Разбор where/set/values без дополнительных библиотек кроме shlex
import shlex

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


def _infer_scalar(token):
    """ true/false/yes/no/1/0 -> bool; целые -> int; остальное -> str. """
    low = token.lower()
    if low in TRUE_TOKENS:
        return True
    if low in FALSE_TOKENS:
        return False
    # целое число с необязательным знаком
    if (low.startswith(("+", "-")) and low[1:].isdigit()) or low.isdigit():
        try:
            return int(low)
        except Exception:
            pass
    return token  # строка (shlex уже снял кавычки)


def _split_assignments(tokens, allowed_sep):
    """Разбирает поток токенов в пары (key, value)."""
    pairs = []
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


def parse_set(text):
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    normalized = text.replace(",", f" {SEP_COMMA} ")
    tokens = shlex.split(normalized, posix=True)
    # унифицируем разделитель
    tokens = [SEP_AND if t == SEP_COMMA else t for t in tokens]
    pairs = _split_assignments(tokens, allowed_sep=SEP_AND)

    out = {}
    for k, raw in pairs:
        if not k:
            raise ValueError(ERR_EMPTY_KEY)
        if k in out:
            raise ValueError(ERR_DUPLICATE_KEYS.format(k))
        out[k] = _infer_scalar(raw)
    return out


def parse_where(text):
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    tokens = shlex.split(text, posix=True)
    pairs = _split_assignments(tokens, allowed_sep=SEP_AND)

    out = {}
    for k, raw in pairs:
        if not k:
            raise ValueError(ERR_EMPTY_KEY)
        if k in out:
            raise ValueError(ERR_DUPLICATE_KEYS.format(k))
        out[k] = _infer_scalar(raw)
    return out


def parse_values_list(text):
    if not text or not text.strip():
        raise ValueError(ERR_EMPTY)

    text = text.strip()
    # обрезаем внешние скобки, если есть
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()

    # Критично: отделяем запятые пробелами, чтобы shlex видел их как отдельные токены
    normalized = text.replace(",", " , ")
    tokens = shlex.split(normalized, posix=True)

    # выбрасываем запятые и приводим типы (int/bool/str)
    clean_tokens = [t for t in tokens if t != ","]
    return [_infer_scalar(t) for t in clean_tokens]
