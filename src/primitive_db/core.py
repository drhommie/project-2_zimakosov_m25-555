# src/primitive_db/core.py
ALLOWED_TYPES = {"int", "str", "bool"}

def _parse_columns(specs):
    """
    Преобразует список вроде ["name:str", "age:int"] в список пар [("name","str"), ("age","int")].
    Если что-то не так — бросает ValueError с нужным сообщением.
    """
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
        if name == "ID":
            raise ValueError(f"Некорректное значение: {name}. Попробуйте снова.")
        if typ not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {typ}. Попробуйте снова.")
        if name in seen:
            raise ValueError(f"Некорректное значение: {name}. Попробуйте снова.")
        seen.add(name)
        parsed.append((name, typ))
    return parsed

def create_table(metadata, table_name, column_specs):
    """
    Создаёт таблицу:
    - имя таблицы должно быть новым,
    - типы столбцов только из ALLOWED_TYPES,
    - в начало добавляется ID:int.
    Возвращает обновлённый metadata или бросает KeyError/ValueError.
    """
    tables = metadata.setdefault("tables", {})
    if table_name in tables:
        raise KeyError(f'Таблица "{table_name}" уже существует.')

    parsed_columns = _parse_columns(column_specs)
    tables[table_name] = {
        "columns": [("ID", "int")] + parsed_columns
    }
    return metadata

def drop_table(metadata, table_name):
    """
    Удаляет таблицу. Если не существует — KeyError.
    """
    tables = metadata.setdefault("tables", {})
    if table_name not in tables:
        raise KeyError(f'Таблица "{table_name}" не существует.')
    del tables[table_name]
    return metadata

def list_tables(metadata):
    """
    Возвращает список имён таблиц (отсортированный).
    """
    tables = metadata.get("tables", {})
    return sorted(tables.keys())
