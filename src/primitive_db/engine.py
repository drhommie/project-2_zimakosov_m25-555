# src/primitive_db/engine.py
import shlex
from .utils import load_metadata, save_metadata
from .core import create_table, drop_table, list_tables

META_FILE = "db_meta.json"

def print_help():
    """Печатает справку по доступным командам."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def run():
    print("***База данных***")
    print_help()
    while True:
        try:
            raw = input(">>>Введите команду: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        try:
            args = shlex.split(raw)
        except ValueError:
            print("Некорректное значение: парсинг команды. Попробуйте снова.")
            continue

        cmd = args[0]

        if cmd == "create_table":
            if len(args) < 2:
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.")
                continue
            table_name = args[1]
            column_specs = args[2:]
            metadata = load_metadata(META_FILE)
            try:
                metadata = create_table(metadata, table_name, column_specs)
                save_metadata(META_FILE, metadata)
                cols = metadata["tables"][table_name]["columns"]
                cols_text = ", ".join([f"{n}:{t}" for n, t in cols])
                print(f'Таблица "{table_name}" успешно создана со столбцами: {cols_text}')
            except KeyError as e:
                print(f"Ошибка: {e.args[0]}")
            except ValueError as e:
                print(str(e))

        elif cmd == "drop_table":
            if len(args) != 2:
                print("Некорректное значение: неверное количество аргументов. Попробуйте снова.")
                continue
            table_name = args[1]
            metadata = load_metadata(META_FILE)
            try:
                metadata = drop_table(metadata, table_name)
                save_metadata(META_FILE, metadata)
                print(f'Таблица "{table_name}" успешно удалена.')
            except KeyError as e:
                print(f"Ошибка: {e.args[0]}")

        elif cmd == "list_tables":
            metadata = load_metadata(META_FILE)
            names = list_tables(metadata)
            for n in names:
                print(f"- {n}")

        elif cmd == "help":
            print_help()

        elif cmd == "exit":
            break

        else:
            print(f"Функции {cmd} нет. Попробуйте снова.")
