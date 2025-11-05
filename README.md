## Управление таблицами

Команды:
- create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> ... — создать таблицу (ID:int добавляется автоматически)
- list_tables — показать список всех таблиц
- drop_table <имя_таблицы> — удалить таблицу
- help — справка
- exit — выход

Пример:
>>> database
>>>Введите команду: create_table users name:str age:int is_active:bool
Таблица "users" успешно создана со столбцами: ID:int, name:str, age:int, is_active:bool

>>>Введите команду: create_table users name:str
Ошибка: Таблица "users" уже существует.

>>>Введите команду: list_tables
- users

>>>Введите команду: drop_table users
Таблица "users" успешно удалена.

>>>Введите команду: drop_table products
Ошибка: Таблица "products" не существует.
