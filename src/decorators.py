import time
from functools import wraps
from typing import Any, Callable
import prompt


def handle_db_errors(func: Callable) -> Callable:
    """
    Перехватывает ошибки KeyError, ValueError, FileNotFoundError.
    Если функция работает с метаданными или данными таблицы,
    возвращает первый аргумент функции при ошибке, чтобы не было None.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            print(f"Ошибка: {e}")
            return args[0] if args else None
        except ValueError as e:
            print(f"Некорректное значение: {e}")
            return args[0] if args else None
        except FileNotFoundError as e:
            print(f"Файл не найден: {e}")
            return args[0] if args else None
    return wrapper


def confirm_action(action_name: str):
    def deco(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ans = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            )
            if ans.lower() != "y":
                print("Операция отменена пользователем.")
                # возвращаем первый аргумент функции, если есть, чтобы не было None
                return args[0] if args else None
            return func(*args, **kwargs)
        return wrapper
    return deco


def log_time(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result
    return wrapper


def create_cacher():
    cache: dict = {}

    def cache_result(key: str, value_func: Callable[[], Any] = None, invalidate: bool = False):
        if invalidate:
            cache.pop(key, None)
            return None
        if key in cache:
            return cache[key]
        if value_func is None:
            return None
        val = value_func()
        cache[key] = val
        return val

    return cache_result