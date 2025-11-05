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
