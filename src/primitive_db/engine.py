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
    print("\n***Управление таблицами***")
    print("Функции:")
    print("create_table <имя_таблицы> <столбец1:тип> "
          "<столбец2:тип> .. - создать таблицу")
    print("list_tables - показать список всех таблиц")
    print("drop_table <имя_таблицы> - удалить таблицу")

    print("\n***Операции с данными***")
    print("insert into <имя_таблицы> values (<значение1>, <значение2>, ...) "
    "- создать запись")
    print("select from <имя_таблицы> - прочитать все записи")
    print("select from <имя_таблицы> where <столбец> = <значение> "
    "- прочитать записи по условию")
    print(
        "update <имя_таблицы> set <столбец> = <новое_значение> "
        "where <столбец> = <значение> - обновить запись"
    )
    print("delete from <имя_таблицы> where <столбец> = <значение> - удалить запись")
    print("info <имя_таблицы> - информация о таблице")

    print("\n***Общие команды***")
    print("help - справочная информация")
    print("exit - выход из программы\n")



def run():
    print("DB project (CRUD) is running!")
    while True:
        meta_path = _get_meta_path()
        metadata = utils.load_metadata(meta_path) or {}

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
            raw_values = values_str.strip("()").split(",")
            values = [v.strip() for v in raw_values]

            table_data = utils.load_table_data(table)
            table_data = core.insert(metadata, table, values, table_data)
            utils.save_table_data(table, table_data)
            utils.save_metadata(_get_meta_path(), metadata)
            print(
                f"Запись с ID={table_data[-1]['ID']} успешно "
                f"добавлена в таблицу \"{table}\"."
            )

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
                    # конвертируем значения к int/bool если нужно
                    for k, v in where.items():
                        col_type = next(
                            (
                                c["type"]
                                for c in metadata[table]["columns"]
                                if c["name"] == k
                            ),
                            "str",
                        )
                        if col_type == "int":
                            where[k] = int(v)
                        elif col_type == "bool":
                            where[k] = v.lower() == "true"
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
                # присваиваем результат
                new_data = core.update(data, set_clause, where_clause)
                if new_data is not None:
                    utils.save_table_data(table, new_data)
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
                new_data = core.delete(data, where)
                if new_data is not None:
                    utils.save_table_data(table, new_data)
                    print(f"Записи из таблицы \"{table}\" успешно удалены.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # INFO
        if cmd == "info" and len(parts) == 2:
            table = parts[1]
            data = utils.load_table_data(table)
            core.info(metadata, table, data)
            continue

        # CREATE TABLE
        if cmd == "create_table" and len(parts) >= 2:
            table = parts[1]
            columns = parts[2:]
            result = core.create_table(metadata, table, columns)
            if result is not None:
                metadata = result
                utils.save_metadata(meta_path, metadata)
                cols_str = ", ".join(
                    f"{c['name']}:{c['type']}" for c in metadata[table]["columns"]
                )
                print(f'Таблица "{table}" успешно создана со столбцами: {cols_str}')
            continue

        # DROP TABLE
        if cmd == "drop_table" and len(parts) == 2:
            table = parts[1]
            metadata = core.drop_table(metadata, table)
            utils.save_metadata(meta_path, metadata)
            print(f'Таблица "{table}" успешно удалена.')
            continue

        # LIST TABLES
        if cmd == "list_tables":
            tables = core.list_tables(metadata)
            if not tables:
                print("(нет таблиц)")
            else:
                for t in tables:
                    print(f"- {t}")
            continue

        print(f"Функции {cmd} нет. Попробуйте снова.")


if __name__ == "__main__":
    run()
