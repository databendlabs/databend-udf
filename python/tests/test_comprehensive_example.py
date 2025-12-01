"""
Comprehensive test suite demonstrating all supported data types.
Tests basic functionality of comprehensive server examples.
"""

import pytest


@pytest.fixture
def comprehensive_server():
    """Comprehensive server with all type examples."""
    import os
    from tests.conftest import ServerManager

    script_path = os.path.join(
        os.path.dirname(__file__), "servers", "comprehensive_server.py"
    )
    manager = ServerManager(script_path)

    if not manager.start():
        pytest.fail("Failed to start comprehensive server")

    yield manager
    manager.stop()


def test_boolean_type(comprehensive_server):
    """Test BOOLEAN type."""
    client = comprehensive_server.get_client()
    assert client.call_function("test_boolean", True) == [False]
    assert client.call_function("test_boolean", False) == [True]


def test_integer_types(comprehensive_server):
    """Test integer types."""
    client = comprehensive_server.get_client()
    assert client.call_function("test_tinyint", 10) == [20]
    assert client.call_function("test_int", 5) == [25]


def test_string_type(comprehensive_server):
    """Test VARCHAR type."""
    client = comprehensive_server.get_client()
    assert client.call_function("test_varchar", "hello") == ["HELLO"]


def test_nullable_with_skip(comprehensive_server):
    """Test nullable input with skip_null=True."""
    client = comprehensive_server.get_client()
    assert client.call_function("test_nullable_skip", 5) == [10]
    assert client.call_function("test_nullable_skip", None) == [None]


def test_nullable_with_handle(comprehensive_server):
    """Test nullable input with skip_null=False."""
    client = comprehensive_server.get_client()
    assert client.call_function("test_nullable_handle", 5) == [10]
    assert client.call_function("test_nullable_handle", None) == [0]


def test_table_function(comprehensive_server):
    """Test table-valued function returning multiple columns."""
    client = comprehensive_server.get_client()

    result = client.call_function_batch(
        "test_table_function", ids=[1, 2, 3], multipliers=[10, 20, 30]
    )

    assert len(result) == 3
    assert result[0] == {"id": 1, "value": "item_1", "squared": 10}
    assert result[1] == {"id": 2, "value": "item_2", "squared": 80}
    assert result[2] == {"id": 3, "value": "item_3", "squared": 270}
