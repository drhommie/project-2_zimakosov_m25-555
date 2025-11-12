# Основная бизнес-логика: управление таблицами и CRUD
from .decorators import handle_db_errors, confirm_action, log_time
import time

from .constants import ALLOWED_TYPES, ID_COL, LOG_TIMINGS, TRUE_TOKENS, FALSE_TOKENS

def _parse_columns(specs):
    if not specs:
        raise ValueError("Некорректное значение: отсутствуют столбцы. Попробуйте снова.")

    parsed = []
    seen = set()

    for token in specs:
        if ":" not in token:
            raise ValueError(f"Некорректное значение: {token}. Попробуйте снова.")
        name, typ = token.split(":", 1)
        name = name.strip()
        typ = typ.strip()

        if not name or not typ:
            raise ValueError(f"Некорректное значение: {token}. Попробуйте снова.")
        if name == ID_COL:
            raise ValueError(f"Некорректное значение: {name}. Попробуйте снова.")
        if typ not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {typ}. Попробуйте снова.")
        if name in seen:
            raise ValueError(f"Некорректное значение: {name}. Попробуйте снова.")
        seen.add(name)
        parsed.append((name, typ))
    return parsed


@handle_db_errors
def create_table(metadata, table_name, column_specs):
    tables = metadata.setdefault("tables", {})
    if table_name in tables:
        raise KeyError(f'Таблица "{table_name}" уже существует.')

    parsed_columns = _parse_columns(column_specs)
    tables[table_name] = {
        "columns": [(ID_COL, "int")] + parsed_columns
    }
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    tables = metadata.setdefault("tables", {})
    if table_name not in tables:
        raise KeyError(f'Таблица "{table_name}" не существует.')
    del tables[table_name]
    return metadata


@handle_db_errors
def list_tables(metadata):
    tables = metadata.get("tables", {})
    return sorted(tables.keys())

def _timed(op_name):
    """Декоратор: простое логирование времени выполнения операции (по флагу)."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            t0 = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                if LOG_TIMINGS:
                    print(f"[{op_name}] {time.time() - t0:.3f}s")
        return wrapper
    return deco


def _rows_io():
    cache = {}

    def load(table_name):
        if table_name not in cache:
            from .utils import load_table_data
            try:
                data = load_table_data(table_name)
            except FileNotFoundError:
                data = []
            if data is None:
                data = []
            if not isinstance(data, list):
                raise ValueError("Повреждённый файл данных: ожидался список строк.")
            cache[table_name] = data
        return cache[table_name]

    def save(table_name, rows):
        cache[table_name] = rows
        from .utils import save_table_data
        save_table_data(table_name, rows)

    return load, save


_load_rows, _save_rows = _rows_io()


def _get_columns(metadata, table_name):
    tables = metadata.get("tables", {})
    if table_name not in tables:
        raise KeyError(f'Таблица "{table_name}" не существует.')
    cols = tables[table_name].get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f'У таблицы "{table_name}" отсутствует корректная схема.')
    return cols


def _data_columns(columns):
    return [c for c in columns if c[0] != ID_COL]


def _coerce(value, type_name):
    if type_name == "int":
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            v = value.strip()
            try:
                return int(v)
            except Exception:
                pass
        raise ValueError(f'Не удалось привести "{value}" к int')
    if type_name == "str":
        return value if isinstance(value, str) else str(value)
    if type_name == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int,)):
            return bool(value)
        if isinstance(value, str):
            v = value.strip().lower()
            if v in TRUE_TOKENS:
                return True
            if v in FALSE_TOKENS:
                return False
        raise ValueError(f'Не удалось привести "{value}" к bool')
    raise ValueError(f"Неподдерживаемый тип столбца: {type_name}")


def _validate_values(columns, values):
    data_cols = _data_columns(columns)
    if len(values) != len(data_cols):
        raise ValueError(f"Ожидается {len(data_cols)} значений (без ID), получено {len(values)}.")

    row = {}
    for (col_name, col_type), raw_val in zip(data_cols, values):
        if col_type not in ALLOWED_TYPES:
            raise ValueError(f"Неподдерживаемый тип столбца: {col_type}")
        row[col_name] = _coerce(raw_val, col_type)
    return row


def _next_id(rows):
    """Генерация нового ID: max(IDs) + 1 или 1, если данных нет."""
    if not rows:
        return 1
    try:
        return max(int(r.get(ID_COL, 0)) for r in rows) + 1
    except Exception:
        return 1


def _match_where(row, where):
    if not where:
        return True
    for k, v in where.items():
        if row.get(k, object()) != v:
            return False
    return True


@handle_db_errors
@log_time
@_timed("insert")
def insert(metadata, table_name, values):
    """Добавляет запись, сохраняет и возвращает обновлённые данные."""
    columns = _get_columns(metadata, table_name)
    new_row_wo_id = _validate_values(columns, values)

    rows = _load_rows(table_name)
    new_id = _next_id(rows)
    new_row = {ID_COL: new_id, **new_row_wo_id}

    rows.append(new_row)
    _save_rows(table_name, rows)
    return rows


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    if not where_clause:
        return list(table_data)
    return [row for row in table_data if _match_where(row, where_clause)]


def _require_where(fn):
    def wrapper(table_data, where_clause=None, *args, **kwargs):
        if not where_clause:
            raise ValueError("WHERE-клаузу необходимо указать (не может быть пустой).")
        return fn(table_data, where_clause, *args, **kwargs)
    return wrapper


@_require_where
def _update_impl(table_data, where_clause, set_clause):
    if not set_clause:
        raise ValueError("SET-клауза пуста — нечего обновлять.")
    for row in table_data:
        if _match_where(row, where_clause):
            for k, v in set_clause.items():
                row[k] = v
    return table_data


@handle_db_errors
@_timed("update")
def update(table_data, set_clause, where_clause):
    return _update_impl(table_data, where_clause, set_clause)


@_require_where
def _delete_impl(table_data, where_clause):
    kept = [row for row in table_data if not _match_where(row, where_clause)]
    table_data.clear()
    table_data.extend(kept)
    return table_data


@handle_db_errors
@confirm_action("удаление записей")
@_timed("delete")
def delete(table_data, where_clause):
    return _delete_impl(table_data, where_clause)
