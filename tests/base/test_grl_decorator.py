import pytest
from pygrl import (
    BasicStorage,
    SQLite3_Storage,
    Storage,
    GeneralRateLimiter as grl,
    ExceededRateLimitError,
)
import time
from typing import Any, Callable


BS = BasicStorage()
SS = SQLite3_Storage(db_path="./storage.db", table_name="storage", overwrite=True)


@pytest.fixture(autouse=True)
def setup():
    BS.clear()
    SS.clear()
    print("\n======= BEG =======")
    yield
    BS.clear()
    SS.clear()
    print("\n======= END =======")


@pytest.mark.parametrize(
    "storage,max_requests,time_window",
    [(BS, 3, 3), (BS, 10, 5), (BS, 5, 7), (SS, 3, 3), (SS, 10, 5), (SS, 5, 7)],
)
def test_decorated_function_wo_key(
    storage: Storage, max_requests: int, time_window: int
):
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window)
    def function():
        return True

    t1 = time.time()
    # Call the function up to the allowed times (`max_requests`)
    for _ in range(max_requests):
        function()
    # Expect it to fail at max_requests + 1 time
    with pytest.raises(ExceededRateLimitError) as exc_info:
        function()
    # Runtime check
    diff = time.time() - t1
    if diff > time_window:
        raise RuntimeError("Previous steps took longer than the time window")
    # Pause the execution until the `time_window`
    time.sleep(time_window - diff)
    # Expect the rate limit to be lifted
    function()


@pytest.mark.parametrize(
    "storage,max_requests,time_window",
    [(BS, 3, 3), (BS, 10, 4), (BS, 5, 2), (SS, 3, 3), (SS, 10, 4), (SS, 5, 2)],
)
def test_decorated_function_w_same_key(
    storage: Storage, max_requests: int, time_window: int
):
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window)
    def function(**kwargs):  # Add **kwargs to accept `key` as keyword argument
        return True

    t1 = time.time()
    # Call the function up to the allowed times (`max_requests`)
    for _ in range(max_requests):
        function(key="key")
    # Expect it to fail at max_requests + 1 time
    with pytest.raises(ExceededRateLimitError) as exc_info:
        function(key="key")
    # Runtime check
    diff = time.time() - t1
    if diff > time_window:
        raise RuntimeError("Previous steps took longer than the time window")
    # Pause the execution until the `time_window`
    time.sleep(time_window - diff)
    # Expect the rate limit to be lifted
    function(key="key")


@pytest.mark.parametrize(
    "storage,max_requests,time_window,number_of_key", [(BS, 3, 3, 10), (SS, 3, 3, 10)]
)
def test_decorated_function_w_different_key(
    storage: Storage, max_requests: int, time_window: int, number_of_key: int
):
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window)
    def function(**kwargs):  # Add **kwargs to accept `key` as keyword argument
        return True

    start = time.time()
    # Load the rate limiter up to it's threshold
    for _ in range(max_requests):
        # Arrange in such a way to ensure all keys are registered almost the same time
        for key_index in range(number_of_key):
            function(key=f"key:{key_index}")
    # variable `buffer` & `delay` exists for the sake of simulation
    buffer = 0.05  # Warning: Does not scale with the `number_of_key` and `time_window`
    delay = 0.1  # Warning: Does not scale with the `number_of_key` and `time_window`
    # Expect rate limits continue to hold until the time_window is exceeded
    while time.time() - start < time_window - buffer:
        # Assumpt for-loop below require less than `buffer` to finish,
        # Otherwise, rate limiting of the last few keys might be lifted, thus fail the test
        for key_index in range(number_of_key):
            with pytest.raises(ExceededRateLimitError) as exc_info:
                function(key=f"key:{key_index}")
        time.sleep(delay)  # Force a short sleep to reduce resource allocation
    time.sleep(buffer)  # Ensure the time_window is reached
    # Expect rate limits to be lifted
    for key_index in range(number_of_key):
        function(key=f"key:{key_index}")


@pytest.mark.parametrize(
    "storage,max_requests,time_window,key_sequence,key",
    [
        (BS, 3, 4, [1, 2, 1, 2, 3, 3, 1, 2, 3], 2),
        (SS, 3, 4, [1, 2, 1, 2, 3, 3, 1, 2, 3], 2),
        (BS, 3, 4, ["a", "b", "a", "b", "c", "d", "a", "b"], "b"),
        (SS, 3, 4, ["a", "b", "a", "b", "c", "d", "a", "b"], "b"),
    ],
)
def test_decorated_function_w_different_key_sequence(
    storage: Storage,
    max_requests: int,
    time_window: int,
    key_sequence: list,
    key: Any,
):
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window)
    def function(**kwargs):  # Add **kwargs to accept `key` as keyword argument
        return True

    start = time.time()
    # Pre-load the rate limiter with keys
    for k in key_sequence:
        function(key=f"k:{k}")
    # Test
    with pytest.raises(ExceededRateLimitError) as exc_info:
        function(key=f"k:{key}")
    # Runtime check
    diff = time.time() - start
    if diff > time_window:
        raise RuntimeError("Previous steps took longer than the time window")


@pytest.mark.parametrize("storage,kwargs,key_builder", [
    (BS, {"username": "user1", "age": 10}, lambda f, *args, **kwargs: kwargs["username"]),
    (SS, {"topic": "python", "fee": 1000}, lambda f, *args, **kwargs: ','.join([f'{k}={v}' for k, v in kwargs.items()])),
    (BS, {"department": "IT", "level": 3}, lambda f, *args, **kwargs: f"{f.__name__}:{kwargs['department']}"),
    (SS, {"department": "EDU", "level": 4}, lambda f, *args, **kwargs: f"{f.__name__}:{kwargs['department']}"),
])
def test_decorated_function_w_key_builder_kwargs(storage: Storage, kwargs: dict, key_builder: Callable):
    # Parameter
    max_requests = 3
    time_window = 3
    
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window, key_builder=key_builder)
    def function(**kwargs):
        return True

    start = time.time()
    # Call the function up to the maximum number of requests
    for _ in range(max_requests):
        function(**kwargs)
    
    with pytest.raises(ExceededRateLimitError) as exc_info:
        function(**kwargs)
    # Call the function with a different kwargs should not trigger the error
    function(hello="world", my="friend", username="someone", department="General")
    # Runtime check
    diff = time.time() - start
    if diff > time_window:
        raise RuntimeError("Previous steps took longer than the time window")


@pytest.mark.parametrize("storage,args,key_builder", [
    (BS, ("user1", 10), lambda f, *args, **kwargs: args[0]),
    (SS, ("python", 1000), lambda f, *args, **kwargs: args[0]),
    (BS, ("IT", 3), lambda f, *args, **kwargs: f"{f.__name__}:{args[0]}"),
    (SS, ("EDU", 4), lambda f, *args, **kwargs: f"{f.__name__}:{args[1]}"),
    (BS, ("IT", 3, False), lambda f, *args, **kwargs: ",".join(list(map(lambda x: str(x), args)))),
    (SS, ("EDU", 4, True), lambda f, *args, **kwargs: ",".join(list(map(lambda x: str(x), args)))),
])
def test_decorated_function_w_key_builder_args(storage: Storage, args: tuple, key_builder: Callable):
    # Parameter
    max_requests = 3
    time_window = 3
    
    # Define a decorated function
    @grl.general_rate_limiter(storage, max_requests, time_window, key_builder=key_builder)
    def function(*args):
        return True

    start = time.time()
    # Call the function up to the maximum number of requests
    for _ in range(3):
        function(*args)
    
    with pytest.raises(ExceededRateLimitError) as exc_info:
        function(*args)
    
    # Call the function with a different args should not trigger the error
    function(100, 200, 300)
    # Runtime check
    diff = time.time() - start
    if diff > time_window:
        raise RuntimeError("Previous steps took longer than the time window")
