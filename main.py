from pygrl import (
    SQLite3_Storage, GeneralRateLimiter as grl, ExceededRateLimitError
)
import random


def example_1(name: str = "example_1"):
    print(f"\n{name}:")

    storage = SQLite3_Storage("storage1.db", overwrite=True)
    rate_limiter = grl(storage, 10, 1)
    try:
        for i in range(12):
            allowed_to_pass = rate_limiter.check_limit("client-key")
            if allowed_to_pass:
                print(f"Request {i + 1}: Allowed")
            else:
                print(f"Request {i + 1}: Exceeded rate limit")
    except Exception as e:
        print(f"Rate limit exceeded: {e}")


def example_2(name: str = "example_2"):
    print(f"\n{name}:")

    @grl.general_rate_limiter(storage=SQLite3_Storage("storage2.db", overwrite=True), max_requests=10, time_window=1)
    def fn(a, b):
        return a + b

    try:
        for i in range(12):
            result = fn(i, i + 1)
            print(f"Result {i + 1}: {result}")
    except ExceededRateLimitError as e:
        print(f"Rate limit exceeded: {e}")


def example_3(name: str = "example_3"):
    print(f"\n{name}:")

    @grl.general_rate_limiter(storage=SQLite3_Storage("storage3.db", overwrite=True), max_requests=2, time_window=1)
    def connect(key: str, host: str, port: int):
        return f"{key} connected to {host}:{port}"

    users = ["Alice", "Bob", "Charlie", "David", "Eve"]
    try:
        for i in range(12):
            user = random.choice(users)
            result = connect(key=user, host="localhost", port=3306)
            print(f"Result: {result}")
    except ExceededRateLimitError as e:
        print(f"Rate limit exceeded: {e}")


def main():
    # example_1("Check limit with SQLite3_Storage")
    # example_2("Apply rate limiter decorator with SQLite3_Storage")
    example_3("Apply rate limiter decorator with SQLite3_Storage (Keyed function)")


if __name__ == "__main__":
    main()
