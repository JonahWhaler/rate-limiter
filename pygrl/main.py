import asyncio
from time import time
from typing import Optional
from .custom_exception import ExceededRateLimitError
from .storage import Storage


class GeneralRateLimiter:
    """
    Rate limiter for general purpose.

    Notes:
    ------
    - DB interaction is not asynchronous!!!
    """
    def __init__(
            self, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 10
    ):
        self.__storage = storage
        self.__max_requests = max_requests
        self.__time_window = time_window
        self.__capacity = max_capacity
        self.__cleanup_threshold = cleanup_threshold if cleanup_threshold > time_window else time_window

    def check_limit(self, key: str) -> bool:
        """
        Checks if a `key` has exceeded the rate limit.

        If the `key`'s first request is being made or if the time window has passed since the first request,
        it resets the start time and number of requests. Otherwise, it increments the number of requests.

        Parameters
        ----------
        key : str
            The key to check the rate limit for.

        Returns
        -------
        bool
            True if the key has not exceeded the rate limit, False otherwise.
        """
        current_time = time()
        item = self.__storage.get(key)
        if item is None:
            item = {"start_time": current_time, "num_requests": 1}
            self.__storage.set(key, item)
            return True

        if current_time - item.get("start_time") > self.__time_window:
            item = {"start_time": current_time, "num_requests": 1}
            self.__storage.set(key, item)
            return True

        item["num_requests"] += 1

        self.__storage.set(key, item)
        return item["num_requests"] <= self.__max_requests

    def cleanup(self):
        keys = self.__storage.keys()
        if len(keys) <= self.__capacity:
            return None

        current_time = time()
        for key in keys:
            item = self.__storage.get(key)
            if current_time - item.get("start_time") > self.__cleanup_threshold:
                self.__storage.drop(key)
        return None

    def __call__(self, key: str) -> bool:
        return_value = self.check_limit(key)

        self.cleanup()

        return return_value

    def reset(self):
        self.__storage.clear()
    
    def info(self) -> dict:
        keys: list = self.__storage.keys()
        values: list = list(map(lambda key: self.__storage.get(key), keys))
        return {"keys": keys, "values": values}
    
    @classmethod
    def general_rate_limiter(
            cls, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 0.1,
            key_builder: Optional[callable] = None
    ):
        """
        Decorator to limit the number of requests to a function.

        Parameters
        ----------
        storage : Storage
            The storage to use to store the number of requests made.
        max_requests : int
            The maximum number of requests a client can make within the time window.
        time_window : int
            The time window in seconds in which the number of requests is limited, default is 1 second.
        max_capacity : int
            The maximum number of keys to store in the storage.
        cleanup_threshold : float
            The threshold to clean up the storage.
        key_builder: callable
            The function to build the key from the function and arguments.
        
        Returns
        -------
        function : The decorated function.

        Raises
        ------
        ExceededRateLimitError : If the rate limit is exceeded.

        Notes:
        ------
        - The key is the name of the function by default.
        - The key can be passed as a keyword argument to the function if rate limiting is required for different keys.
        - `key_builder` has a higher priority than the `key` argument.
        - `key_builder` should be a function that returns a string.
        """

        def decorator(func):
            limiter = GeneralRateLimiter(storage, max_requests, time_window, max_capacity, cleanup_threshold)

            def wrapper(*args, **kwargs):
                if key_builder:
                    key = key_builder(func, *args, **kwargs)
                elif kwargs.get("key"):
                    key = kwargs.get("key")
                else:
                    key = f"{func.__name__}"
                if not limiter(key):
                    raise ExceededRateLimitError(
                        f"Rate limit exceeded. "
                        f"`{key}` was/had called more than {max_requests} requests per {time_window} seconds."
                    )
                return func(*args, **kwargs)

            return wrapper

        return decorator


class GeneralRateLimiter_with_Lock:
    """
    Rate limiter for general purpose.
    Core operations are guarded by asyncio.Lock().

    Notes:
    ------
    - DB interaction is not asynchronous!!!
    """
    def __init__(
            self, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 10
    ):
        self.__storage = storage
        self.__max_requests = max_requests
        self.__time_window = time_window
        self.__capacity = max_capacity
        self.__cleanup_threshold = cleanup_threshold if cleanup_threshold > time_window else time_window
        self.__lock = asyncio.Lock()

    async def check_limit(self, key: str) -> bool:
        async with self.__lock:
            current_time = time()
            item = self.__storage.get(key)
            if item is None:
                item = {"start_time": current_time, "num_requests": 1}
                self.__storage.set(key, item)
                return True

            if current_time - item.get("start_time") > self.__time_window:
                item = {"start_time": current_time, "num_requests": 1}
                self.__storage.set(key, item)
                return True

            item["num_requests"] += 1

            self.__storage.set(key, item)
            return item["num_requests"] <= self.__max_requests

    async def cleanup(self):
        async with self.__lock:
            keys = self.__storage.keys()

            if len(keys) <= self.__capacity:
                return None

            current_time = time()
            for key in keys:
                item = self.__storage.get(key)
                if current_time - item.get("start_time") > self.__cleanup_threshold:
                    self.__storage.drop(key)
            return None

    async def __call__(self, key: str) -> bool:
        return_value = await self.check_limit(key)

        await self.cleanup()

        return return_value

    async def reset(self):
        async with self.__lock:
            self.__storage.clear()
    
    async def info(self) -> dict:
        async with self.__lock:
            keys: list = self.__storage.keys()
            values: list = list(map(lambda key: self.__storage.get(key), keys))
            return {"keys": keys, "values": values}
    
    @classmethod
    def general_rate_limiter(
            cls, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 0.1,
            key_builder: Optional[callable] = None
    ):
        """
        Decorator to limit the number of requests to a function.

        Parameters
        ----------
        storage : Storage
            The storage to use to store the number of requests made.
        max_requests : int
            The maximum number of requests a client can make within the time window.
        time_window : int
            The time window in seconds in which the number of requests is limited, default is 1 second.
        max_capacity : int
            The maximum number of keys to store in the storage.
        cleanup_threshold : float
            The threshold to clean up the storage.
        key_builder: callable
            The function to build the key from the function and arguments.
        
        Returns
        -------
        function : The decorated function.

        Raises
        ------
        ExceededRateLimitError : If the rate limit is exceeded.

        Notes:
        ------
        - The key is the name of the function by default.
        - The key can be passed as a keyword argument to the function if rate limiting is required for different keys.
        - `key_builder` has a higher priority than the `key` argument.
        - `key_builder` should be a function that returns a string.
        """

        def decorator(func):
            limiter = GeneralRateLimiter_with_Lock(storage, max_requests, time_window, max_capacity, cleanup_threshold)

            async def wrapper(*args, **kwargs):
                if key_builder:
                    key = key_builder(func, *args, **kwargs)
                elif kwargs.get("key"):
                    key = kwargs.get("key")
                else:
                    key = f"{func.__name__}"
                if not await limiter(key):
                    raise ExceededRateLimitError(
                        f"Rate limit exceeded. "
                        f"`{key}` was/had called more than {max_requests} requests per {time_window} seconds."
                    )
                return await func(*args, **kwargs)

            return wrapper

        return decorator


if __name__ == "__main__":
    pass
