![my-logo](https://jonahtzuchi.github.io/rate-limiter/logo-mini.jpg "pygrl-Logo")

# PYGRL - Python General Rate Limiter
Another Python package aim to offer "Rate Limiting" functionality for general use cases.

# Features
- Flexible storage strategy (Memory | File | Database)
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

# Example - GeneralRateLimiter

```python
from pygrl import BasicStorage, GeneralRateLimiter, ExceededRateLimitError
import random
from typing import Optional

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
```