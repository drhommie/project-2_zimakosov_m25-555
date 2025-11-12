"""
constants.py — централизованное хранилище всех констант проекта.
"""

import os

# --- файлы и директории ---
META_FILE = "db_meta.json"
DATA_DIR = "data"

# --- типы и поля ---
ALLOWED_TYPES = {"int", "str", "bool"}
ID_COL = "ID"

# --- булевые значения ---
TRUE_TOKENS = {"true", "1", "yes", "y"}
FALSE_TOKENS = {"false", "0", "no", "n"}

# --- поведение CLI ---
SHOW_HELP = os.environ.get("DB_SHOW_HELP", "1") == "1"
LOG_TIMINGS = False  # включение/выключение замера времени для CRUD операций
