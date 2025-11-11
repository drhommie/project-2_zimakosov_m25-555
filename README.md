# Примитивная база данных (CLI)

Мини‑проект учебной базы данных на Python с консольным интерфейсом. 
Поддерживает создание/удаление таблиц, хранение данных в JSON, и полный набор операций **CRUD** — `insert`, `select`, `update`, `delete`. 
В проекте настроены линтер **Ruff** и автотесты. Также добавлены декораторы для обработки ошибок, логирования времени и подтверждения опасных действий.

## Возможности
- Создание и удаление таблиц со схемой столбцов (`int`, `str`, `bool`).  
- Хранение данных в `data/<table>.json`.  
- CRUD‑операции с простым SQL‑подобным синтаксисом.  
- Красивый вывод с **PrettyTable**.  
- Декораторы: централизованная обработка ошибок, замер времени, подтверждение опасных операций.  

---

## Установка

Требования: **Python 3.12+**, **Poetry 2.x**

### Вариант A — через Poetry
```bash
poetry install
```

### Вариант B — через Makefile
```bash
make install
```

---

## Запуск

### Вариант A — через Poetry
```bash
poetry run database
```

### Вариант B — через Makefile
```bash
make run
```

После запуска вы попадёте в CLI‑интерфейс с приглашением к вводу команд.

---

## Быстрый старт (полный сценарий)

Ниже пример последовательности, покрывающей весь цикл: создание таблицы → добавление → чтение → обновление → удаление → удаление таблицы.

```text
# Создаём таблицу со схемой: ID:int добавляется автоматически
create_table users name:str age:int is_active:bool

# Добавляем записи
insert into users values ("Anton", 28, true)
insert into users values ("Alla", 31, false)

# Выбираем данные
select from users
select from users where age = 28 and is_active = true

# Обновляем данные
update users set age = 29 where name = "Anton"

# Удаляем записи
delete from users where name = "Alla"

# Информация о таблице
info users

# Удаляем таблицу
drop_table users
```

> Примечания:
> - Строки — в кавычках (`"text"`). Булевы значения: `true/false`, `yes/no`, `1/0`.
> - Данные сохраняются автоматически после `insert/update/delete`.
> - Для схемы таблиц используются типы: `int`, `str`, `bool`.

---

## Демонстрация (asciinema)

Полный сценарий работы показан в демонстрации ниже (встроенная запись):
[![asciinema demo (full)](https://asciinema.org/a/usWjkW7rcvGQMxjbfEWCqFkll.svg)](https://asciinema.org/a/usWjkW7rcvGQMxjbfEWCqFkll)

Дополнительно (по шагам):
- Управление таблицами (создание/список/удаление):  
  [![asciinema demo](https://asciinema.org/a/sETP3BZ7z3s51MklTQWhIKxll.svg)](https://asciinema.org/a/sETP3BZ7z3s51MklTQWhIKxll)
- CRUD‑операции:  
  [![asciinema demo](https://asciinema.org/a/9vrWjyQki3q9l3MMc8ZXMc7A0.svg)](https://asciinema.org/a/9vrWjyQki3q9l3MMc8ZXMc7A0)

---

## Полезные команды разработчика

Запуск линтера Ruff:
```bash
make lint
# или
poetry run ruff check .
```

Запуск тестов (если предусмотрено в проекте):
```bash
make test
# или
poetry run pytest -q
```

---

## Структура проекта (сокращённо)
```
project-root/
├─ src/
│  ├─ __init__.py
│  ├─ decorators.py          # Декораторы (обработка ошибок, логирование, подтверждения)
│  └─ primitive_db/
│     ├─ __init__.py
│     ├─ core.py            # CRUD-логика и работа с таблицами
│     ├─ engine.py          # Парсинг и диспетчеризация команд
│     ├─ main.py            # CLI-интерфейс (точка входа)
│     ├─ parser.py          # Разбор пользовательских запросов
│     └─ utils.py           # Вспомогательные функции
├─ Makefile                  # Команды установки, запуска и линтинга
├─ pyproject.toml            # Настройки Poetry, зависимости, entry point
├─ poetry.lock               # Зафиксированные версии библиотек
├─ .gitignore                # Список исключений из репозитория
└─ README.md                 # Документация проекта
```

---

## Технические детали

- **Схемы таблиц** описаны в meta‑файле (например, `db_meta.json`), при создании таблицы к схеме всегда добавляется `ID:int` (автоинкремент).
- **Вывод таблиц** реализован с помощью библиотеки PrettyTable.
- **Кэширование select** может быть реализовано замыканием для повторных запросов.
- **Обработка ошибок** централизована декоратором, чтобы не дублировать `try/except`.
- **Подтверждения** запрашиваются перед опасными действиями (например, `drop_table`, массовый `delete`).