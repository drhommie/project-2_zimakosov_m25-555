import json
import os

DATA_DIR = "data"


def load_metadata(filepath):
    """Читает JSON с метаданными. Если файла нет — возвращает {"tables": {}}."""
    if not os.path.exists(filepath):
        return {"tables": {}}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "tables" not in data or not isinstance(data["tables"], dict):
            return {"tables": {}}
        return data
    except FileNotFoundError:
        return {"tables": {}}


def save_metadata(filepath, data):
    """Сохраняет словарь метаданных в JSON с отступами."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _table_path(table_name):
    filename = f"{table_name}.json"
    return os.path.join(DATA_DIR, filename)


def load_table_data(table_name):
    """Загружает список записей таблицы из data/<table>.json. Если файла нет — []."""
    _ensure_data_dir()
    path = _table_path(table_name)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_table_data(table_name, data):
    """Сохраняет список записей таблицы в data/<table>.json."""
    _ensure_data_dir()
    path = _table_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
