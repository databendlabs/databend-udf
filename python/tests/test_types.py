"""
Tests for data type handling.

This module tests:
- Unit tests: Type parsing, formatting, VECTOR type
- Unit tests: Table function output formats (RecordBatch, dict iterables)
- Integration tests: End-to-end type handling via Flight
"""

import datetime
from decimal import Decimal

import pyarrow as pa

from databend_udf import Headers, udf
from databend_udf.udf import (
    _type_str_to_arrow_field,
    _field_type_to_string,
    _input_process_func,
    _output_process_func,
    _arrow_field_to_string,
)


# =============================================================================
# Test Helpers
# =============================================================================


def _make_input_batch(func, columns):
    arrays = [
        pa.array(values, type=field.type)
        for values, field in zip(columns, func._input_schema)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=func._input_schema)


def _batch_to_rows(batch):
    data = batch.to_pydict()
    keys = list(data.keys())
    rows = []
    for idx in range(batch.num_rows):
        rows.append({key: data[key][idx] for key in keys})
    return rows


def _collect_rows(func, batch):
    rows = []
    for out_batch in func.eval_batch(batch, Headers()):
        rows.extend(_batch_to_rows(out_batch))
    return rows


# =============================================================================
# Unit Tests: VECTOR Type
# =============================================================================


def test_vector_sql_generation():
    """Test VECTOR type SQL generation."""
    # Test nullable VECTOR (default)
    field = _type_str_to_arrow_field("VECTOR(1024)")
    sql_type = _arrow_field_to_string(field)
    assert sql_type == "VECTOR(1024)"

    # Test NOT NULL VECTOR
    field_not_null = _type_str_to_arrow_field("VECTOR(1024) NOT NULL")
    sql_type_not_null = _arrow_field_to_string(field_not_null)
    assert sql_type_not_null == "VECTOR(1024) NOT NULL"


def test_vector_type_parsing():
    """Test VECTOR type parsing to Arrow."""
    field = _type_str_to_arrow_field("VECTOR(1024)")
    # Should be FixedSizeList type with metadata
    assert pa.types.is_fixed_size_list(field.type)
    assert field.type.list_size == 1024
    assert field.metadata[b"Extension"] == b"Vector"
    assert field.metadata[b"vector_size"] == b"1024"
    assert pa.types.is_float32(field.type.value_type)
    # Default is nullable
    assert field.nullable is True

    # Test NOT NULL
    field_not_null = _type_str_to_arrow_field("VECTOR(1024) NOT NULL")
    assert field_not_null.nullable is False


def test_vector_type_formatting():
    """Test VECTOR Arrow type formatting back to SQL."""
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True), list_size=1024),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"1024",
        },
    )
    type_str = _field_type_to_string(field)
    assert type_str == "VECTOR(1024)"


def test_vector_input_processing():
    """Test VECTOR input processing."""
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True), list_size=3),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"3",
        },
    )
    func = _input_process_func(field)
    data = [[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]]
    processed = func(data)
    assert processed == data


def test_vector_output_processing():
    """Test VECTOR output processing."""
    field = pa.field(
        "",
        pa.list_(pa.field("item", pa.float32(), nullable=True), list_size=3),
        nullable=False,
        metadata={
            b"Extension": b"Vector",
            b"vector_size": b"3",
        },
    )
    func = _output_process_func(field)
    data = [[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]]
    processed = func(data)
    assert processed == data


# =============================================================================
# Unit Tests: Table Function Output Formats
# =============================================================================


@udf(
    input_types=["INT"],
    result_type=[("value", "BIGINT"), ("square", "BIGINT")],
    batch_mode=True,
)
def table_returns_record_batch(nums):
    """Table UDF that returns a RecordBatch directly."""
    schema = pa.schema([pa.field("value", pa.int64()), pa.field("square", pa.int64())])
    return pa.RecordBatch.from_arrays(
        [
            pa.array(nums, type=pa.int64()),
            pa.array([n * n for n in nums], type=pa.int64()),
        ],
        schema=schema,
    )


@udf(
    input_types=["INT", "INT"],
    result_type=[("left", "INT"), ("right", "INT"), ("sum", "INT")],
    batch_mode=True,
)
def table_returns_iterable(lhs, rhs):
    """Table UDF that returns an iterable of dicts."""
    return [
        {"left": left_value, "right": right_value, "sum": left_value + right_value}
        for left_value, right_value in zip(lhs, rhs)
    ]


def test_table_function_accepts_record_batch():
    """Test table function returning RecordBatch."""
    batch = _make_input_batch(table_returns_record_batch, [[1, 2, 3]])
    rows = _collect_rows(table_returns_record_batch, batch)
    assert rows == [
        {"value": 1, "square": 1},
        {"value": 2, "square": 4},
        {"value": 3, "square": 9},
    ]


def test_table_function_accepts_iterable_of_dicts():
    """Test table function returning iterable of dicts."""
    batch = _make_input_batch(table_returns_iterable, [[1, 2], [10, 20]])
    rows = _collect_rows(table_returns_iterable, batch)
    assert rows == [
        {"left": 1, "right": 10, "sum": 11},
        {"left": 2, "right": 20, "sum": 22},
    ]


# =============================================================================
# Integration Tests: End-to-end Type Handling
# =============================================================================


def test_scalar_types(types_server):
    """Test basic scalar type handling."""
    client = types_server.get_client()

    # Boolean
    assert client.call_function("negate_bool", True) == [False]
    assert client.call_function("negate_bool", False) == [True]

    # Integer types
    assert client.call_function("double_tinyint", 10) == [20]
    assert client.call_function("square_int", 5) == [25]
    assert client.call_function("increment_bigint", 100) == [101]

    # Float types
    assert client.call_function("half_float", 10.0) == [5.0]
    assert client.call_function("double_double", 2.5) == [5.0]


def test_string_and_binary(types_server):
    """Test string and binary type handling."""
    client = types_server.get_client()

    assert client.call_function("upper_varchar", "hello") == ["HELLO"]
    assert client.call_function("reverse_binary", b"abc") == [b"cba"]


def test_decimal_type(types_server):
    """Test decimal type handling."""
    client = types_server.get_client()

    result = client.call_function("scale_decimal", Decimal("10.00"))[0]
    assert result == Decimal("15.00")


def test_date_and_timestamp(types_server):
    """Test date and timestamp type handling."""
    client = types_server.get_client()

    assert client.call_function("next_day", datetime.date(2024, 1, 1)) == [
        datetime.date(2024, 1, 2)
    ]

    ts = datetime.datetime(2024, 1, 1, 12, 0, 0)
    result = client.call_function("add_hour", ts)[0]
    assert result.hour == 13


def test_complex_types(types_server):
    """Test complex type handling (array, map)."""
    client = types_server.get_client()

    # Array - need to use batch mode for array input
    result = client.call_function_batch("double_array", arr=[[1, 2, 3]])
    assert result == [[2, 4, 6]]

    # Map - returned as list of tuples, convert to dict for comparison
    result = client.call_function_batch("double_map_values", m=[{"a": 1, "b": 2}])
    assert dict(result[0]) == {"a": 2, "b": 4}


def test_nullable_types(types_server):
    """Test nullable type handling."""
    client = types_server.get_client()

    # skip_null=True: returns doubled value
    assert client.call_function("nullable_double", 5) == [10]

    # skip_null=False: returns 0 for None, doubled for value
    assert client.call_function("nullable_or_zero", 5) == [10]
