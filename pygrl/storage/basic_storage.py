from collections import defaultdict
from typing import Any
from .storage import Storage


class BasicStorage(Storage):
    """
    Basic storage class that stores key-value pairs in memory without any persistence.
    
    Notes:
    Expect the keys to be string, or at least convertible to strings.
    """

    def __init__(self):
        # print("BasicStorage init")
        f: float | None = None
        self.__memory = defaultdict(lambda: {"start_time": f, "num_requests": 0})

    def get(self, key: str):
        # Force the type of the key to string
        if type(key) is not str:
            key = str(key)
        return self.__memory.get(key)

    def set(self, key: str, value: Any):
        # Force the type of the key to string
        if type(key) is not str:
            key = str(key)
        self.__memory.update({key: value})

    def drop(self, key: str):
        # Force the type of the key to string
        if type(key) is not str:
            key = str(key)
        self.__memory.pop(key, None)

    def clear(self):
        self.__memory.clear()

    def keys(self) -> list[str]:
        return list(self.__memory.keys())
