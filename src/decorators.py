from functools import wraps
import time

def handle_db_errors(func):
    """
    Декоратор для централизованной обработки ошибок в операциях БД.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, база данных не инициализирована.")
        except KeyError as e:
            msg = str(e)
            if "не существует" in msg or "уже существует" in msg:
                print(f"Ошибка: {msg}")
            else:    
                print(f"Ошибка: Таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
        return None
    return wrapper


def confirm_action(action_name: str):
    """
    Фабрика декораторов: перед выполнением функции спрашивает подтверждение.
    Если ответ не 'y' — операция отменяется (возвращает None).
    """
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ').strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nОперация отменена.")
                return None
            if answer != "y":
                print("Операция отменена пользователем.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return deco


def log_time(func):
    """
    Декоратор: измеряет время выполнения функции и печатает результат.
    Использует time.monotonic() для точности.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f'Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.')
        return result
    return wrapper
