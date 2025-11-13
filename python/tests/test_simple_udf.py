"""
Simple UDF function tests.
Tests basic user-defined function capability.
"""


def test_simple_addition(basic_server):
    """Test simple integer addition UDF."""
    client = basic_server.get_client()
    result = client.call_function("add_two_ints", 2, 3)
    assert result == [5]


def test_gcd_function(basic_server):
    """Test GCD function."""
    client = basic_server.get_client()
    result = client.call_function("gcd", 48, 18)
    assert result == [6]


def test_batch_function_calls(basic_server):
    """Test batch function calls with named arguments."""
    client = basic_server.get_client()

    # Test batch call with multiple values
    result = client.call_function_batch("add_two_ints", a=[1, 2, 3], b=[4, 5, 6])
    assert result == [5, 7, 9]

    # Test batch GCD
    result = client.call_function_batch("gcd", x=[48, 56], y=[18, 21])
    assert result == [6, 7]


def test_batch_validation(basic_server):
    """Test batch function validation."""
    client = basic_server.get_client()

    # Test missing required argument - should be caught by client validation
    try:
        client.call_function_batch("add_two_ints", a=[1, 2])  # Missing 'b'
        assert False, "Should have raised ValueError for missing argument"
    except ValueError as e:
        assert "Missing required arguments" in str(e)
    except Exception as e:
        # If server catches it first, that's also acceptable
        assert "missing" in str(e).lower()

    # Test extra argument - should be caught by client validation
    try:
        client.call_function_batch(
            "add_two_ints", a=[1, 2], b=[3, 4], c=[5, 6]
        )  # Extra 'c'
        assert False, "Should have raised ValueError for extra argument"
    except ValueError as e:
        assert "Unexpected arguments" in str(e)


def test_positional_argument_validation(basic_server):
    """Test positional argument count validation."""
    client = basic_server.get_client()

    # Test too few arguments
    try:
        client.call_function("add_two_ints", 5)  # Missing second argument
        assert False, "Should have raised ValueError for wrong argument count"
    except ValueError as e:
        assert "expects 2 arguments, got 1" in str(e)

    # Test too many arguments
    try:
        client.call_function("add_two_ints", 1, 2, 3)  # Extra argument
        assert False, "Should have raised ValueError for wrong argument count"
    except ValueError as e:
        assert "expects 2 arguments, got 3" in str(e)


def test_batch_array_length_validation(basic_server):
    """Test batch array length consistency validation."""
    client = basic_server.get_client()

    # Test inconsistent array lengths
    try:
        client.call_function_batch(
            "add_two_ints", a=[1, 2, 3], b=[4, 5]
        )  # Different lengths
        assert False, "Should have raised ValueError for inconsistent array lengths"
    except ValueError as e:
        assert "All batch arrays must have the same length" in str(e)


def test_table_function_returns_records(basic_server):
    """Test table-valued function returning multiple columns."""
    client = basic_server.get_client()

    batch_result = client.call_function_batch("expand_numbers", nums=[1, 2, 3])
    assert batch_result == [
        {"num": 1, "double_num": 2},
        {"num": 2, "double_num": 4},
        {"num": 3, "double_num": 6},
    ]

    single_result = client.call_function("expand_numbers", 5)
    assert single_result == [{"num": 5, "double_num": 10}]
