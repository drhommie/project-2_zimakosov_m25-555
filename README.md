# Примитивная база данных

## 1. Подготовка

Создайте проект с использованием Poetry 2.x, настройте `src-layout`, линтер Ruff и CLI-команду `database`.

## 2. База данных

На этом этапе реализовано создание, удаление и просмотр таблиц.

### Команды:
 - create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> ... — создать таблицу (ID:int добавляется автоматически)
 - list_tables — показать список всех таблиц
 - drop_table <имя_таблицы> — удалить таблицу
 - help — справка
 - exit — выход
 
**Пример:**
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
 
### Демонстрация
 
 [![asciinema demo](https://asciinema.org/a/sETP3BZ7z3s51MklTQWhIKxll.svg)](https://asciinema.org/a/sETP3BZ7z3s51MklTQWhIKxll)
 
## 3. CRUD-операции

Теперь реализован полный набор операций с данными — **CRUD** (Create, Read, Update, Delete).

### Общие сведения

Каждая таблица хранится в файле `data/<имя_таблицы>.json`.
Для вывода используется библиотека **PrettyTable**.

---

### insert

Создаёт новую запись в таблице.

**Синтаксис:**
```bash
insert into <имя_таблицы> values (<значение1>, <значение2>, ...)
```

**Пример:**
```bash
insert into users values ("Sergei", 28, true)
```

**Результат:**
```
Запись с ID=1 успешно добавлена в таблицу "users".
```

---

### select

Отображает данные из таблицы.

**Синтаксис:**
```bash
select from <имя_таблицы>
select from <имя_таблицы> where <колонка> = <значение> [and <колонка> = <значение> ...]
```

**Пример:**
```bash
select from users where age = 28
```

**Результат:**
```
+----+--------+-----+-----------+
| ID |  name  | age | is_active |
+----+--------+-----+-----------+
| 1  | Sergei | 28  |    True   |
+----+--------+-----+-----------+
```

---

### update

Изменяет существующие записи.

**Синтаксис:**
```bash
update <имя_таблицы> set <колонка1> = <новое_значение1>[, <колонка2> = <новое_значение2>] where <колонка_условия> = <значение>
```

**Пример:**
```bash
update users set age = 29 where name = "Sergei"
```

**Результат:**
```
Запись(и) в таблице "users" успешно обновлена(ы).
```

---

### delete

Удаляет записи по условию.

**Синтаксис:**
```bash
delete from <имя_таблицы> where <колонка> = <значение>
```

**Пример:**
```bash
delete from users where ID = 1
```

**Результат:**
```
Запись(и) успешно удалена(ы) из таблицы "users".
```

---

### info

Отображает схему таблицы и количество записей.

**Синтаксис:**
```bash
info <имя_таблицы>
```

**Пример:**
```bash
info users
```

**Результат:**
```
Таблица: users
Столбцы: ID:int, name:str, age:int, is_active:bool
Количество записей: 0
```

---

### Примечания

- Строки обязательно в кавычках.
- Булевы значения: `true/false`, `yes/no`, `1/0`.
- Все изменения сохраняются автоматически после `insert`, `update` и `delete`.
- Для вывода таблиц используется `prettytable`.

---

Теперь CLI поддерживает весь цикл:
- `create_table`, `drop_table`, `list_tables`
- `insert`, `select`, `update`, `delete`, `info`

### Демонстрация
 
 [![asciinema demo](https://asciinema.org/a/9vrWjyQki3q9l3MMc8ZXMc7A0.svg)](https://asciinema.org/a/9vrWjyQki3q9l3MMc8ZXMc7A0)