"""
Tests for UDF function calling and validation.

This module tests:
- Basic UDF function calls (single and batch)
- Argument validation (count, types, batch lengths)
- Table function output formats
"""


def test_simple_addition(types_server):
    """Test simple integer addition UDF."""
    client = types_server.get_client()
    result = client.call_function("add_ints", 2, 3)
    assert result == [5]


def test_gcd_function(types_server):
    """Test GCD function."""
    client = types_server.get_client()
    result = client.call_function("gcd", 48, 18)
    assert result == [6]


def test_batch_function_calls(types_server):
    """Test batch function calls with named arguments."""
    client = types_server.get_client()

    # Test batch call with multiple values
    result = client.call_function_batch("add_ints", a=[1, 2, 3], b=[4, 5, 6])
    assert result == [5, 7, 9]

    # Test batch GCD
    result = client.call_function_batch("gcd", x=[48, 56], y=[18, 21])
    assert result == [6, 7]


def test_batch_validation(types_server):
    """Test batch function validation."""
    client = types_server.get_client()

    # Test missing required argument - should be caught by client validation
    try:
        client.call_function_batch("add_ints", a=[1, 2])  # Missing 'b'
        assert False, "Should have raised ValueError for missing argument"
    except ValueError as e:
        assert "Missing required arguments" in str(e)
    except Exception as e:
        # If server catches it first, that's also acceptable
        assert "missing" in str(e).lower()

    # Test extra argument - should be caught by client validation
    try:
        client.call_function_batch(
            "add_ints", a=[1, 2], b=[3, 4], c=[5, 6]
        )  # Extra 'c'
        assert False, "Should have raised ValueError for extra argument"
    except ValueError as e:
        assert "Unexpected arguments" in str(e)


def test_positional_argument_validation(types_server):
    """Test positional argument count validation."""
    client = types_server.get_client()

    # Test too few arguments
    try:
        client.call_function("add_ints", 5)  # Missing second argument
        assert False, "Should have raised ValueError for wrong argument count"
    except ValueError as e:
        assert "expects 2 arguments, got 1" in str(e)

    # Test too many arguments
    try:
        client.call_function("add_ints", 1, 2, 3)  # Extra argument
        assert False, "Should have raised ValueError for wrong argument count"
    except ValueError as e:
        assert "expects 2 arguments, got 3" in str(e)


def test_batch_array_length_validation(types_server):
    """Test batch array length consistency validation."""
    client = types_server.get_client()

    # Test inconsistent array lengths
    try:
        client.call_function_batch(
            "add_ints", a=[1, 2, 3], b=[4, 5]
        )  # Different lengths
        assert False, "Should have raised ValueError for inconsistent array lengths"
    except ValueError as e:
        assert "All batch arrays must have the same length" in str(e)


def test_table_function_returns_records(types_server):
    """Test table-valued function returning multiple columns."""
    client = types_server.get_client()

    batch_result = client.call_function_batch(
        "expand_pairs", lhs=[1, 2, 3], rhs=[10, 20, 30]
    )
    assert batch_result == [
        {"left": 1, "right": 10, "sum": 11},
        {"left": 2, "right": 20, "sum": 22},
        {"left": 3, "right": 30, "sum": 33},
    ]

    single_result = client.call_function("expand_pairs", 4, 6)
    assert single_result == [{"left": 4, "right": 6, "sum": 10}]
