#!/usr/bin/env python3
import prompt


def print_help() -> None:
    """Печатает краткую справку для первого запуска."""
    print("\n<command> exit - выйти из программы")
    print("<command> help - справочная информация")


def welcome() -> None:
    """Первый экран: приветствие, help/exit, цикл чтения команд."""
    print("Первая попытка запустить проект!\n")
    print("***")
    print_help()

    while True:
        cmd = prompt.string("Введите команду: ").strip().lower()

        if cmd == "exit":
            # выходим
            break
        elif cmd == "help":
            print()
            print_help()
        elif cmd == "":
            # пустой ввод — повторить запрос
            continue
        else:
            continue
