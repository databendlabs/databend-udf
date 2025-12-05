"""
Tests for UDF server basic functionality.

This module tests:
- Server startup and process management
- Health check endpoint
- Built-in functions (healthy, echo)
"""


def test_server_can_start(minimal_server):
    """Test that UDF server can start successfully."""
    assert minimal_server.process is not None
    assert minimal_server.process.poll() is None


def test_health_check(minimal_server):
    """Test server responds to health check."""
    client = minimal_server.get_client()
    assert client.health_check() is True


def test_builtin_healthy(minimal_server):
    """Test built-in healthy function returns 1."""
    client = minimal_server.get_client()
    result = client.call_function("builtin_healthy")
    assert result == [1]


def test_builtin_echo(minimal_server):
    """Test built-in echo function works."""
    client = minimal_server.get_client()
    result = client.call_function("builtin_echo", "hello")
    assert result == ["hello"]
