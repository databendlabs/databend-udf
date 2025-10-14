"""Integration tests against the self-contained demo server."""

import datetime
from decimal import Decimal


def test_simple_functions(demo_server):
    client = demo_server.get_client()
    assert client.call_function("add_four", 1, 2, 3, 4) == [10]
    assert client.call_function("select_if", True, 5, 6) == [5]


def test_strings_and_binary(demo_server):
    client = demo_server.get_client()
    assert client.call_function("split_and_join", "a,b", ",", ":") == ["a:b"]
    assert client.call_function("reverse_bytes", b"xyz") == [b"zyx"]


def test_decimal_and_dates(demo_server):
    client = demo_server.get_client()
    dec = client.call_function("decimal_div", Decimal("1"), Decimal("3"))[0]
    assert dec == Decimal("0.3333333333333333333333333333")
    assert client.call_function("add_days", datetime.date(2024, 1, 1), 2) == [
        datetime.date(2024, 1, 3)
    ]


def test_collections(demo_server):
    client = demo_server.get_client()
    array_result = client.call_function_batch(
        "array_access", values=[["foo", "bar"]], index=[2]
    )
    assert array_result == ["bar"]
    assert client.call_function("map_lookup", {"x": "y"}, "x") == ["y"]


def test_wait_and_tuple(demo_server):
    client = demo_server.get_client()
    assert client.call_function("wait", 9) == [9]
    tuple_result = client.call_function_batch(
        "tuple_slice", values=[["a", "b", "c"]], i=[0], j=[2]
    )
    assert tuple_result == [["a", "c"]]


def test_return_all_types(demo_server):
    client = demo_server.get_client()
    values = [
        True,
        -1,
        2,
        3,
        4,
        1.5,
        2.5,
        datetime.date(2024, 2, 1),
        datetime.datetime(2024, 2, 1, 12, 30, 0),
        "hello",
        '{"k":"v"}',
    ]
    import json

    snapshot_raw = client.call_function("all_types_snapshot", *values)[0]
    if isinstance(snapshot_raw, (bytes, bytearray)):
        snapshot = json.loads(snapshot_raw.decode("utf-8"))
    elif isinstance(snapshot_raw, str):
        snapshot = json.loads(snapshot_raw)
    else:
        snapshot = snapshot_raw
    assert snapshot["flag"] is True
    assert snapshot["tiny"] == -1
    assert snapshot["text"] == "hello"
    assert snapshot["date"] == "2024-02-01"
