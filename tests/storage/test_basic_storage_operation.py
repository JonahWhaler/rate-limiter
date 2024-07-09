import pytest
from pygrl import BasicStorage
from time import time


@pytest.fixture
def basic_storage():
    return BasicStorage()


@pytest.mark.parametrize("key", ["key", "client", "temporary", 1, 2, 3])
def test_bs_get(basic_storage, key):
    assert basic_storage.get(key) is None


@pytest.mark.parametrize("key,num_requests", [
    ("key", 1),
    ("client", 10),
    ("admin", 3)
])
def test_bs_set_single(basic_storage, key, num_requests):
    """
    Test the `get` method.
    
    Environment:
    ------------
    - Pre-load the same item into the storage container.
    """
    input_value = {"start_time": time(), "num_requests": num_requests}
    basic_storage.set(key, input_value)
    output_value = basic_storage.get(key)
    assert input_value == output_value


@pytest.mark.parametrize("keys,values,key,value", [
    (["a", "b", "c", "d"], [(100, 1), (201, 23), (823, 12), (123, 20)], "a", {"start_time": 100, "num_requests": 1}),
    (["a", "b", "c", "d", "e"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32)], "e", {"start_time": 234, "num_requests": 32}),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], "c", {"start_time": 823, "num_requests": 12}),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], "g", None),
    (["a", "b", "c", "d"], [(100, 1), (201, 23), (823, 12), (123, 20)], "e", None)
])
def test_bs_get_complex(basic_storage, keys: list, values: list, key: str, value: dict):
    """
    Test the `get` method.
    
    Environment:
    ------------
    - Pre-load multiple items into the storage container.
    """
    for k, v in zip(keys, values):
        basic_storage.set(k, {"start_time": v[0], "num_requests": v[1]})
        
    output_value = basic_storage.get(key)
    assert output_value == value


@pytest.mark.parametrize("keys,values,key", [
    (["a", "b", "c", "d"], [(100, 1), (201, 23), (823, 12), (123, 20)], "a"),
    (["a", "b", "c", "d", "e"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32)], "e"),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], "c"),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], "z")
])
def test_bs_drop(basic_storage, keys: list, values: list, key: str):
    for k, v in zip(keys, values):
        basic_storage.set(k, {"start_time": v[0], "num_requests": v[1]})
    basic_storage.drop(key)
    for k in keys:
        value = basic_storage.get(k)
        if k == key:
            assert value is None
        else:
            assert value is not None


@pytest.mark.parametrize("keys,values", [
    (["a", "b", "c"], [(100, 1), (201, 23), (823, 12)]),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)])
])
def test_bs_clear(basic_storage, keys: list, values: list):
    for k, v in zip(keys, values):
        basic_storage.set(k, {"start_time": v[0], "num_requests": v[1]})
    basic_storage.clear()
    for k in keys:
        assert basic_storage.get(k) is None


@pytest.mark.parametrize("keys,values,expected", [
    (["a", "b", "c"], [(100, 1), (201, 23), (823, 12)], ["a", "b", "c"]),
    (["z", "y", "x"], [(100, 1), (201, 23), (823, 12)], ["z", "y", "x"]),
    (["a", "w", "f", "d", "k", "e"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], ["a", "w", "f", "d", "k", "e"]),
    (["a", "b", "c", "d", "e", "f"], [(100, 1), (201, 23), (823, 12), (123, 20), (234, 32), (239, 12)], ["a", "b", "c", "d", "e", "f"]),
    ([1, 2, 3], [(100, 1), (201, 23), (823, 12)], ["1", "2", "3"]),
    ([10, 5, 8], [(100, 1), (201, 23), (823, 12)], ["10", "5", "8"])
])
def test_bs_keys(basic_storage, keys: list, values: list, expected: list):
    for k, v in zip(keys, values):
        basic_storage.set(k, {"start_time": v[0], "num_requests": v[1]})
    intersect = set(basic_storage.keys()) & set(expected)
    assert len(intersect) == len(expected)
