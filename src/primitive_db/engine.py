# src/primitive_db/engine.py

import shlex
from prettytable import PrettyTable
from typing import Any, Callable, Hashable
import json
import hashlib

from .utils import (
    load_metadata,
    save_metadata,
    load_table_data,
    save_table_data,
)
from .core import (
    create_table,
    drop_table,
    list_tables,
    insert as core_insert,
    select as core_select,
    update as core_update,
    delete as core_delete,
)
from .parser import parse_where, parse_set, parse_values_list

# Именованная константа вместо "магической строки"
META_FILE = "db_meta.json"

# --- кэширование результатов select (замыкание) ---

def create_cacher():
    """
    Возвращает функцию cache_result(key, value_func),
    которая кэширует результат по ключу в замыкании.
    """
    cache: dict[Hashable, Any] = {}

    def cache_result(key: Hashable, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        val = value_func()
        cache[key] = val
        return val

    return cache_result

_SELECT_CACHE = create_cacher()

def _rows_digest(rows: list[dict[str, Any]]) -> str:
    """
    Детеминированный отпечаток текущего содержимого таблицы.
    Если в строках попадутся несерилизуемые типы — приводим к str.
    """
    try:
        payload = json.dumps(rows, sort_keys=True, ensure_ascii=False)
    except TypeError:
        safe_rows = [
            {k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v))
             for k, v in row.items()}
            for row in rows
        ]
        payload = json.dumps(safe_rows, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def print_help():
    """Печатает справку по доступным командам."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу (альтернатива)")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop table <имя_таблицы> - удалить таблицу")
    print("<command> drop_table <имя_таблицы> - удалить таблицу (альтернатива)")
    print("\n***Операции с данными***")
    print("<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись")
    print("<command> select from <имя_таблицы> - прочитать все записи")
    print("<command> select from <имя_таблицы> where <колонка> = <значение> [and ...] - выбрать по условию")
    print("<command> update <имя_таблицы> set <k1> = <v1>[, k2 = v2] where <колонка> = <значение> [and ...] - обновить")
    print("<command> delete from <имя_таблицы> where <колонка> = <значение> [and ...] - удалить")
    print("<command> info <имя_таблицы> - вывести информацию о таблице")
    print("\nОбщие команды:")
    print("<command> help - справочная информация")
    print("<command> exit - выход из программы\n")


# ---- Вспомогательные функции для CRUD ----

def _get_columns_from_metadata(metadata, table_name):
    """
    Возвращает список пар (name, type) в том же формате, как в core.create_table:
      [("ID","int"), ("name","str"), ...]
    """
    tables = metadata.get("tables", {})
    if table_name not in tables:
        raise KeyError(f'Таблица "{table_name}" не существует.')
    cols = tables[table_name].get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f'У таблицы "{table_name}" отсутствует корректная схема.')
    return cols


def _render_select(rows, columns):
    """
    Печатает результат SELECT в виде таблицы.
    Порядок столбцов соответствует схеме (ID, затем остальные).
    """
    headers = [c[0] for c in columns]
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row([row.get(col, "") for col in headers])
    print(table)


# ---- Обработчики новых команд (используем raw_line, чтобы не терять кавычки) ----

def _handle_insert(metadata, raw_line: str):
    # Формат: insert into <table> values ("str with spaces", 123, true)
    parts = shlex.split(raw_line, posix=True)
    if len(parts) < 4 or parts[0].lower() != "insert" or parts[1].lower() != "into":
        raise ValueError("Некорректная команда INSERT. Ожидается: insert into <table> values (<значения>)")

    table_name = parts[2]

    low = raw_line.lower()
    vidx = low.find("values")
    if vidx == -1:
        raise ValueError("Отсутствует секция values(...)")

    payload = raw_line[vidx + len("values"):].strip()
    if payload.startswith("(") and payload.endswith(")"):
        payload = payload[1:-1].strip()

    values = parse_values_list(payload)

    updated = core_insert(metadata, table_name, values)
    if updated is None:
        return
    save_table_data(table_name, updated)

    last_id = max((r.get("ID", 0) for r in updated), default=0)
    print(f'Запись с ID={last_id} успешно добавлена в таблицу "{table_name}".')


def _handle_select(metadata, raw_line: str):
    # Формат: select from <table> [where <expr>]
    parts = shlex.split(raw_line, posix=True)
    if len(parts) < 3 or parts[0].lower() != "select" or parts[1].lower() != "from":
        raise ValueError("Некорректная команда SELECT. Ожидается: select from <table> [where <условие>]")

    table_name = parts[2]

    where_clause = None
    low = raw_line.lower()
    widx = low.find(" where ")
    if widx != -1:
        expr = raw_line[widx + len(" where "):].strip()
        where_clause = parse_where(expr)

    # 1) Сначала проверяем, что таблица существует и берём схему
    try:
        columns = _get_columns_from_metadata(metadata, table_name)
    except KeyError as e:
        # Сообщение уже человекочитаемое: 'Таблица "X" не существует.'
        print(f"Ошибка: {e.args[0]}")
        return
    except ValueError as e:
        print(str(e))
        return

    # 2) Загружаем строки и вызываем core.select (он уже под декоратором)
    rows = load_table_data(table_name)
    result = core_select(rows, where_clause)
    if result is None:
        return

    # 3) Рендер
    _render_select(result, columns)


def _handle_update(metadata, raw_line: str):
    # Формат: update <table> set <...> where <...>
    parts = shlex.split(raw_line, posix=True)
    if len(parts) < 5 or parts[0].lower() != "update":
        raise ValueError("Некорректная команда UPDATE. Ожидается: update <table> set <...> where <...>")

    table_name = parts[1]

    low = raw_line.lower()
    sidx = low.find(" set ")
    widx = low.find(" where ")
    if sidx == -1 or widx == -1 or widx < sidx:
        raise ValueError("Для UPDATE требуются секции SET и WHERE.")

    set_expr = raw_line[sidx + len(" set "): widx].strip()
    where_expr = raw_line[widx + len(" where "):].strip()

    set_clause = parse_set(set_expr)
    where_clause = parse_where(where_expr)

    rows = load_table_data(table_name)
    updated = core_update(rows, set_clause, where_clause)
    if updated is None:
        return
    save_table_data(table_name, updated)
    print(f'Запись(и) в таблице "{table_name}" успешно обновлена(ы).')


def _handle_delete(metadata, raw_line: str):
    # Формат: delete from <table> where <...>
    parts = shlex.split(raw_line, posix=True)
    if len(parts) < 4 or parts[0].lower() != "delete" or parts[1].lower() != "from":
        raise ValueError("Некорректная команда DELETE. Ожидается: delete from <table> where <условие>")

    table_name = parts[2]

    low = raw_line.lower()
    widx = low.find(" where ")
    if widx == -1:
        raise ValueError("Для DELETE требуется секция WHERE.")

    where_expr = raw_line[widx + len(" where "):].strip()
    where_clause = parse_where(where_expr)

    rows = load_table_data(table_name)
    updated = core_delete(rows, where_clause)
    if updated is None:
        return
    save_table_data(table_name, updated)
    print(f'Запись(и) успешно удалена(ы) из таблицы "{table_name}".')


def _handle_info(metadata, raw_line: str):
    # Формат: info <table>
    parts = shlex.split(raw_line, posix=True)
    if len(parts) != 2 or parts[0].lower() != "info":
        raise ValueError("Некорректная команда INFO. Ожидается: info <table>")

    table_name = parts[1]
    columns = _get_columns_from_metadata(metadata, table_name)
    rows = load_table_data(table_name)
    cols_str = ", ".join([f"{name}:{typ}" for (name, typ) in columns])
    print(f"Таблица: {table_name}")
    print(f"Столбцы: {cols_str}")
    print(f"Количество записей: {len(rows)}")


def run():
    print("***База данных***")
    print_help()

    while True:
        # --- чтение ввода ---
        try:
            raw = input(">>>Введите команду: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        # --- разбор строки ---
        try:
            args = shlex.split(raw, posix=True)
        except ValueError:
            print("Некорректное значение: парсинг команды. Попробуйте снова.")
            continue

        if not args:
            continue

        # нормализуем регистр первой команды
        args[0] = args[0].lower()

        # --- составные команды: create table / drop table ---
        if len(args) >= 2:
            if args[0] == "create" and args[1].lower() == "table":
                args = ["create_table"] + args[2:]
            elif args[0] == "drop" and args[1].lower() == "table":
                args = ["drop_table"] + args[2:]

        cmd = args[0]

        # ---------------- Управление таблицами ----------------
        if cmd == "create_table":
            if len(args) < 2:
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.")
                continue

            table_name = args[1]
            column_specs = args[2:]  # формат: name:type name:type ...

            metadata = load_metadata(META_FILE)
            updated_meta = create_table(metadata, table_name, column_specs)
            if updated_meta is None:
                # ошибка уже выведена декоратором
                continue

            save_metadata(META_FILE, updated_meta)
            # гарантируем пустой файл данных для новой таблицы
            save_table_data(table_name, [])

            cols = updated_meta["tables"][table_name]["columns"]
            cols_text = ", ".join(f"{n}:{t}" for n, t in cols)
            print(f'Таблица "{table_name}" успешно создана со столбцами: {cols_text}')

        elif cmd == "drop_table":
            if len(args) != 2:
                print("Некорректное значение: неверное количество аргументов. Попробуйте снова.")
                continue

            table_name = args[1]
            metadata = load_metadata(META_FILE)
            updated_meta = drop_table(metadata, table_name)
            if updated_meta is None:
                continue

            save_metadata(META_FILE, updated_meta)
            print(f'Таблица "{table_name}" успешно удалена.')

        elif cmd == "list_tables":
            metadata = load_metadata(META_FILE)
            names = list_tables(metadata) or []
            for n in names:
                print(f"- {n}")

        # ---------------- CRUD-команды ----------------
        elif cmd == "insert":
            metadata = load_metadata(META_FILE)
            _handle_insert(metadata, raw)  # внутри обработчика стоят guard'ы на None

        elif cmd == "select":
            metadata = load_metadata(META_FILE)
            _handle_select(metadata, raw)

        elif cmd == "update":
            metadata = load_metadata(META_FILE)
            _handle_update(metadata, raw)

        elif cmd == "delete":
            metadata = load_metadata(META_FILE)
            _handle_delete(metadata, raw)

        elif cmd == "info":
            metadata = load_metadata(META_FILE)
            _handle_info(metadata, raw)

        elif cmd == "help":
            print_help()

        elif cmd == "exit":
            break

        else:
            print(f"Функции {cmd} нет. Попробуйте снова.")

