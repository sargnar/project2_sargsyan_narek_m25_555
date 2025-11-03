#!/usr/bin/env python3
import shlex
from pathlib import Path

import prompt

from . import core, utils

DB_META_FILENAME = "db_meta.json"


def _get_meta_path() -> Path:
    return Path.cwd() / DB_META_FILENAME


def _parse_condition(expr: str) -> dict:
    if "=" not in expr:
        raise ValueError("Некорректное условие.")
    key, val = [x.strip() for x in expr.split("=", 1)]
    val = val.strip('"')
    return {key: val}


def print_help():
    print("\n***Операции с данными***")
    print("Функции:")
    print("insert into <имя_таблицы> values (<значения>) - создать запись")
    print("select from <имя_таблицы> [where <столбец>=<значение>] - чтение")
    print(
        "update <имя_таблицы> set <столбец>=<новое_значение> "
        "where <столбец>=<значение> - обновить"
    )
    print("delete from <имя_таблицы> where <столбец>=<значение> - удалить")
    print("info <имя_таблицы> - информация о таблице")
    print("help - справка, exit - выход\n")


def run():
    print("DB project (CRUD) is running!")
    while True:
        meta_path = _get_meta_path()
        metadata = utils.load_metadata(meta_path)

        user_input = prompt.string("Введите команду: ")
        if not user_input:
            continue

        parts = shlex.split(user_input)
        if not parts:
            continue

        cmd = parts[0].lower()

        if cmd == "exit":
            print("Выход из программы...")
            break

        if cmd == "help":
            print_help()
            continue

        # INSERT
        if cmd == "insert" and len(parts) >= 4 and parts[1] == "into":
            table = parts[2]
            if parts[3] != "values":
                print("Некорректная команда insert.")
                continue
            values_str = user_input.split("values", 1)[1].strip()
            if not (values_str.startswith("(") and values_str.endswith(")")):
                print("Некорректный формат values.")
                continue
            values = shlex.split(values_str.strip("()"))

            table_data = utils.load_table_data(table)
            try:
                table_data = core.insert(metadata, table, values, table_data)
                utils.save_table_data(table, table_data)
                utils.save_metadata(meta_path, metadata)
                print(
                    f"Запись с ID={table_data[-1]['ID']} "
                    f"успешно добавлена в таблицу \"{table}\"."
                )
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # SELECT
        if cmd == "select" and len(parts) >= 3 and parts[1] == "from":
            table = parts[2]
            table_data = utils.load_table_data(table)

            if "where" in parts:
                where_index = parts.index("where")
                condition_str = " ".join(parts[where_index + 1:])
                try:
                    where = _parse_condition(condition_str)
                    result = core.select(table_data, where)
                except Exception as e:
                    print(f"Ошибка: {e}")
                    continue
            else:
                result = core.select(table_data)

            core.print_table(result)
            continue

        # UPDATE
        if cmd == "update" and len(parts) >= 6:
            table = parts[1]
            if parts[2] != "set" or "where" not in parts:
                print("Некорректная команда update.")
                continue
            where_index = parts.index("where")
            set_expr = " ".join(parts[3:where_index])
            where_expr = " ".join(parts[where_index + 1:])
            try:
                set_clause = _parse_condition(set_expr)
                where_clause = _parse_condition(where_expr)
                data = utils.load_table_data(table)
                data = core.update(data, set_clause, where_clause)
                utils.save_table_data(table, data)
                print(f"Записи в таблице \"{table}\" успешно обновлены.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # DELETE
        if cmd == "delete" and len(parts) >= 5 and parts[1] == "from":
            table = parts[2]
            if parts[3] != "where":
                print("Некорректная команда delete.")
                continue
            cond_expr = " ".join(parts[4:])
            try:
                where = _parse_condition(cond_expr)
                data = utils.load_table_data(table)
                data = core.delete(data, where)
                utils.save_table_data(table, data)
                print(f"Записи из таблицы \"{table}\" успешно удалены.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # INFO
        if cmd == "info" and len(parts) == 2:
            table = parts[1]
            data = utils.load_table_data(table)
            try:
                core.info(metadata, table, data)
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        print(f"Функции {cmd} нет. Попробуйте снова.")


if __name__ == "__main__":
    run()
