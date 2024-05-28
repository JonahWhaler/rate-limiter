![my-logo](https://jonahtzuchi.github.io/rate-limiter/logo-mini.jpg "pygrl-Logo")

# PYGRL - Python General Rate Limiter
Another Python package aim to offer "Rate Limiting" functionality for general use cases.

# Features
- Flexible storage strategy (Memory | File | Database)
  - MemoryStorage
    - `BasicStorage`
  - FileStorage
    - `SQLite3_Storage`
- Cleanup expired rate limiters
- Use as a decorator
- Use as a variable
- Compatible with fastapi (TO BE TESTED)
- Support asynchronous DB operations (TODO)

# Dependencies
- Python 3.10

# Installation
```bash
pip3 install pygrl
```

# Example - BasicStorage

## Imports
```python
from pygrl import BasicStorage, GeneralRateLimiter as grl, ExceededRateLimitError
```

# Check limit with BasicStorage
```python
storage = BasicStorage()
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
```

## Apply rate limiter decorator with BasicStorage
```python
@grl.general_rate_limiter(storage=BasicStorage(), max_requests=10, time_window=1)
def fn(a, b):
    return a + b

try:
    for i in range(12):
        result = fn(i, i + 1)
        print(f"Result {i + 1}: {result}")
except ExceededRateLimitError as e:
    print(f"Rate limit exceeded: {e}")
```

# Apply rate limiter decorator with BasicStorage (Keyed function)
```python
import random

@grl.general_rate_limiter(storage=BasicStorage(), max_requests=2, time_window=1)
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
```

# Example - SQLite3_Storage

## Imports
```python
from pygrl import SQLite3_Storage, GeneralRateLimiter as grl, ExceededRateLimitError
```

## Check limit with SQLite3_Storage
```python
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
```

## Apply rate limiter decorator with SQLite3_Storage
```python
@grl.general_rate_limiter(storage=SQLite3_Storage("storage2.db", overwrite=True), max_requests=10, time_window=1)
def fn(a, b):
    return a + b

try:
    for i in range(12):
        result = fn(i, i + 1)
        print(f"Result {i + 1}: {result}")
except ExceededRateLimitError as e:
    print(f"Rate limit exceeded: {e}")
```

## Apply rate limiter decorator with SQLite3_Storage (Keyed function)
```python
import random

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
```

# Source Code
- https://github.com/JonahTzuChi/rate-limiter
