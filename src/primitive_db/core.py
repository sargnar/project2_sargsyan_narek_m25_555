from typing import Any, Dict, List, Optional

from prettytable import PrettyTable

ALLOWED_TYPES = {"int", "str", "bool"}


def _parse_column_spec(spec: str) -> tuple[str, str]:
    if ":" not in spec:
        raise ValueError(spec)
    name, typ = spec.split(":", 1)
    name = name.strip()
    typ = typ.strip()
    if not name or not typ:
        raise ValueError(spec)
    return name, typ


def create_table(metadata: Dict[str, Any],
                 table_name: str,
                 columns: List[str]) -> Dict[str, Any]:
    if table_name in metadata:
        raise KeyError(f"Таблица \"{table_name}\" уже существует.")

    cols = [{"name": "ID", "type": "int"}]

    for spec in columns:
        name, typ = _parse_column_spec(spec)
        if typ not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {spec}")
        cols.append({"name": name, "type": typ})

    metadata[table_name] = {"columns": cols}
    return metadata


def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if table_name not in metadata:
        raise KeyError(f"Таблица \"{table_name}\" не существует.")
    del metadata[table_name]
    return metadata


def list_tables(metadata: Dict[str, Any]) -> List[str]:
    return list(metadata.keys())


def insert(metadata: Dict[str, Any],
           table_name: str,
           values: List[str],
           table_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if table_name not in metadata:
        raise KeyError(f"Таблица \"{table_name}\" не существует.")

    columns = metadata[table_name]["columns"][1:]
    if len(values) != len(columns):
        raise ValueError("Количество значений не соответствует количеству столбцов.")

    record = {}
    for i, col in enumerate(columns):
        col_type = col["type"]
        val = values[i]
        if col_type == "int":
            try:
                val = int(val)
            except ValueError:
                raise ValueError(f"Некорректное значение: {val}")
        elif col_type == "bool":
            val = val.lower() == "true"
        elif col_type == "str":
            val = val.strip('"')
        record[col["name"]] = val

    new_id = max([r["ID"] for r in table_data], default=0) + 1
    record["ID"] = new_id
    table_data.append(record)
    return table_data


def select(table_data: List[Dict[str, Any]],
           where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    if not where:
        return table_data
    key, value = next(iter(where.items()))
    return [r for r in table_data if str(r.get(key)) == str(value)]


def update(table_data: List[Dict[str, Any]],
           set_clause: Dict[str, Any],
           where: Dict[str, Any]) -> List[Dict[str, Any]]:
    key_w, val_w = next(iter(where.items()))
    for row in table_data:
        if str(row.get(key_w)) == str(val_w):
            for k, v in set_clause.items():
                row[k] = v
    return table_data


def delete(table_data: List[Dict[str, Any]],
           where: Dict[str, Any]) -> List[Dict[str, Any]]:
    key, value = next(iter(where.items()))
    return [r for r in table_data if str(r.get(key)) != str(value)]


def print_table(data: List[Dict[str, Any]]) -> None:
    if not data:
        print("(нет данных)")
        return
    table = PrettyTable()
    table.field_names = data[0].keys()
    for row in data:
        table.add_row(row.values())
    print(table)


def info(metadata: Dict[str, Any],
         table_name: str,
         table_data: List[Dict[str, Any]]) -> None:
    if table_name not in metadata:
        raise KeyError(f"Таблица \"{table_name}\" не существует.")
    cols = ", ".join(
        [f"{c['name']}:{c['type']}" for c in metadata[table_name]["columns"]]
    )
    print(f"Таблица: {table_name}")
    print(f"Столбцы: {cols}")
    print(f"Количество записей: {len(table_data)}")
