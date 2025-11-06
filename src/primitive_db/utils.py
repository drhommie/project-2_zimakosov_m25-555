# src/primitive_db/utils.py
import json
import os

def load_metadata(filepath):
    """
    Читает JSON с метаданными. Если файла нет — возвращает {"tables": {}}.
    """
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
    """
    Сохраняет словарь метаданных в JSON с отступами.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

DATA_DIR = "data"

def _ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

def _table_path(table_name: str) -> str:
    # простая и безопасная сборка пути: data/<table>.json
    filename = f"{table_name}.json"
    return os.path.join(DATA_DIR, filename)

def load_table_data(table_name: str):
    """
    Загружает список записей таблицы (list[dict]) из data/<table>.json.
    Если файла нет — возвращает пустой список.
    """
    _ensure_data_dir()
    path = _table_path(table_name)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_table_data(table_name: str, data) -> None:
    """
    Сохраняет список записей таблицы (list[dict]) в data/<table>.json.
    """
    _ensure_data_dir()
    path = _table_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
