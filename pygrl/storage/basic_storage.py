from collections import defaultdict
from typing import Any
from .storage import Storage


class BasicStorage(Storage):
    """
    Basic storage class that stores key-value pairs in memory without any persistence.
    """

    def __init__(self):
        # print("BasicStorage init")
        f: float | None = None
        self.__memory = defaultdict(lambda: {"start_time": f, "num_requests": 0})

    def get(self, key: str):
        return self.__memory.get(key)

    def set(self, key: str, value: Any):
        self.__memory.update({key: value})

    def drop(self, key: str):
        self.__memory.pop(key, None)

    def clear(self):
        self.__memory.clear()

    def keys(self) -> list[str]:
        return list(self.__memory.keys())
