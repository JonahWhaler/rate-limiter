import asyncio
import random
from time import time
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
        self.__cleanup_threshold = cleanup_threshold

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

    @classmethod
    def general_rate_limiter(
            cls, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 0.1
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
        """

        def decorator(func):
            limiter = GeneralRateLimiter(storage, max_requests, time_window, max_capacity, cleanup_threshold)

            def wrapper(*args, **kwargs):
                key = kwargs.get("key")
                if not key:
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
        self.__cleanup_threshold = cleanup_threshold
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

    @classmethod
    def general_rate_limiter(
            cls, storage: Storage,
            max_requests: int, time_window: int = 1,
            max_capacity: int = 32, cleanup_threshold: float = 0.1
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
        """

        def decorator(func):
            limiter = GeneralRateLimiter_with_Lock(storage, max_requests, time_window, max_capacity, cleanup_threshold)

            async def wrapper(*args, **kwargs):
                key = kwargs.get("key")
                if not key:
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
    # Example 1
    print("\n\n>>Example 1")
    basic_storage = BasicStorage()
    limiter = GeneralRateLimiter(basic_storage, 10, 1)

    for i in range(15):
        print(f"Request {i + 1}: {limiter.check_limit('client_id')}")

    # # Example 2
    print("\n\n>>Example 2 - Apply rate limiter to a function")


    @GeneralRateLimiter.general_rate_limiter(BasicStorage(), 10, 1)
    def compute(x, y):
        return x + y


    exec_counter_1 = 0
    for i in range(15):
        try:
            compute(i, i + 1)
            exec_counter_1 += 1
        except ExceededRateLimitError as e:
            # print(f"At {exec_counter_1} => {e}")
            exec_counter_1 += 1
    print(f"Executed {exec_counter_1} times.")

    # Example 3
    print("\n\n>>Example 3 - Apply rate limiter to a function with different keys")


    @GeneralRateLimiter.general_rate_limiter(BasicStorage(), 10, 1, 2, 1)
    def compute_for_user(x, y, key: Optional[str] = None):
        return x + y


    clients = ["john", "amy", "jane", "joe"]
    saturated_clients = set()
    exec_counter_2 = 0
    for i in range(100):
        client = random.choice(clients)
        try:
            compute_for_user(i, i + 1, key=client)
            exec_counter_2 += 1
        except ExceededRateLimitError as e:
            saturated_clients.add(client)
            if len(saturated_clients) == len(clients):
                print("All clients are saturated.")
                break
            # print(f"At {exec_counter_2} => {e}")
    print(f"Executed {exec_counter_2} times.")

    # Example 4
    # print("\n\n>>Example 4")
    #
    #
    # async def example_4():
    #     basic_storage = BasicStorage()
    #     limiter = GeneralRateLimiter_with_Lock(basic_storage, 10, 1)
    #
    #     results = [limiter.check_limit('client_id') for _ in range(15)]
    #     value = await asyncio.gather(*results)
    #     for i in range(15):
    #         print(i, value[i])
    #
    #
    # # asyncio.run(example_4())
    #
    # # Example 5
    # print("\n\n>>Example 5")
    #
    # @GeneralRateLimiter_with_Lock.general_rate_limiter(BasicStorage(), 10, 1)
    # async def compute(x, y):
    #     return x + y
    #
    # async def example_5():
    #     exec_counter_1 = 0
    #     for i in range(15):
    #         try:
    #             await compute(i, i + 1)
    #             exec_counter_1 += 1
    #         except ExceededRateLimitError as e:
    #             exec_counter_1 += 1
    #             # print(f"At {exec_counter_1} => {e}")
    #     print(f"Executed {exec_counter_1} times.")
    #
    # # asyncio.run(example_5())
    #
    # # Example 6
    # print("\n\n>>Example 6")
    #
    # @GeneralRateLimiter_with_Lock.general_rate_limiter(BasicStorage(), 10, 1, 2, 0.0)
    # async def compute_for_user(x, y, key: Optional[str] = None):
    #     return x + y
    #
    # async def example_6():
    #     client = ["john", "amy", "jane", "joe"]
    #     exec_counter_2 = 0
    #     saturated = set()
    #     for i in range(100):
    #         user = random.choice(client)
    #         try:
    #             await compute_for_user(i, i + 1, key=user)
    #             exec_counter_2 += 1
    #         except ExceededRateLimitError as e:
    #             saturated.add(user)
    #             if len(saturated) == len(client):
    #                 print("All clients are saturated.")
    #                 break
    #             # print(f"At {exec_counter_2} => {e}")
    #     print(f"Executed {exec_counter_2} times.")
    #
    # asyncio.run(example_6())
