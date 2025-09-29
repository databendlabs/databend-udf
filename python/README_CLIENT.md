# Databend UDF Client Library

Simple Python client library for testing Databend UDF servers.

## Installation

```bash
pip install databend-udf
```

## Quick Start

```python
from databend_udf import UDFClient, create_client

# Create client (default: localhost:8815)
client = create_client()

# Or specify host/port
client = UDFClient(host="localhost", port=8815)

# Health check
if client.health_check():
    print("Server is running!")

# Echo test
result = client.echo("Hello, Databend!")
print(result)  # "Hello, Databend!"

# Call UDF function
result = client.call_function("gcd", 48, 18)
print(result[0])  # 6
```

## API Reference

### UDFClient

#### Methods

- `__init__(host="localhost", port=8815)` - Create client
- `health_check() -> bool` - Check server health
- `echo(message: str) -> str` - Echo test message
- `call_function(name, *args) -> List[Any]` - Call UDF with arguments
- `call_function_batch(name, **kwargs) -> List[Any]` - Call UDF with batch data
- `get_function_info(name) -> FlightInfo` - Get function schema
- `list_functions() -> List[str]` - List available functions

### Examples

#### Single Value Calls

```python
client = create_client()

# Numeric functions
result = client.call_function("add_signed", 1, 2, 3, 4)
print(result[0])  # 10

# String functions
result = client.call_function("split_and_join", "a,b,c", ",", "-")
print(result[0])  # "a-b-c"

# Array functions
result = client.call_function("array_access", ["hello", "world"], 1)
print(result[0])  # "hello"
```

#### Batch Calls

```python
client = create_client()

# Process multiple values at once
x_values = [48, 56, 72]
y_values = [18, 21, 24]
results = client.call_function_batch("gcd_batch", a=x_values, b=y_values)
print(results)  # [6, 7, 24]
```

## Testing

Run the example server:

```bash
cd python/example
python server.py
```

In another terminal, run tests:

```bash
# Simple test
python simple_test.py

# Comprehensive test suite
python client_test.py
```

## Error Handling

```python
try:
    result = client.call_function("my_function", arg1, arg2)
    print(result[0])
except Exception as e:
    print(f"Function call failed: {e}")
```

## Performance Testing

The client supports testing concurrent I/O operations:

```python
# Sequential calls (slow)
for i in range(10):
    client.call_function("slow_function", i)

# Batch call (fast with io_threads)
client.call_function_batch("slow_function", a=list(range(10)))
```