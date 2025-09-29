# Databend UDF Tests

Professional UDF test suite with isolated server instances for each test.

## Test Structure

```
tests/
├── conftest.py              # Pytest fixtures and server management
├── test_connectivity.py     # Basic connectivity tests
├── test_simple_udf.py      # Simple UDF function tests
├── servers/
│   ├── minimal_server.py   # Minimal server (built-in functions only)
│   └── basic_server.py     # Basic server (simple arithmetic functions)
```

## Test Principles

1. **Isolation** - Each test runs with its own server instance
2. **Progressive** - Test basic connectivity first, then functionality
3. **Focused** - Test core capabilities only, avoid complex types

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest test_connectivity.py

# Run specific test function
pytest test_connectivity.py::test_health_check
```

## Test Content

### Connectivity Tests
- Server startup
- Health check
- Built-in functions (builtin_healthy, builtin_echo)

### Simple UDF Tests
- Integer addition
- GCD algorithm

Each test uses an isolated server instance to ensure complete isolation between tests.