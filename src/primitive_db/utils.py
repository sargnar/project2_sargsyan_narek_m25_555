import json
from pathlib import Path
from typing import Any, Dict, List


def load_metadata(filepath: str | Path) -> Dict[str, Any]:
    path = Path(filepath)
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str | Path, data: Dict[str, Any]) -> None:
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=4)


def _data_path(table_name: str) -> Path:
    data_dir = Path.cwd() / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir / f"{table_name}.json"


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    path = _data_path(table_name)
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    path = _data_path(table_name)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=4)
