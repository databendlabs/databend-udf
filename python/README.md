# Databend Python UDF

Write user-defined functions in Python for Databend.

## Quick Start

```python
from databend_udf import udf, UDFServer

@udf(input_types=["INT", "INT"], result_type="INT")
def add(a: int, b: int) -> int:
    return a + b

if __name__ == '__main__':
    server = UDFServer("0.0.0.0:8815")
    server.add_function(add)
    server.serve()
```

```bash
python3 my_udf.py
```

```sql
CREATE FUNCTION add (INT, INT) RETURNS INT
LANGUAGE python HANDLER = 'add' ADDRESS = 'http://0.0.0.0:8815';

SELECT add(1, 2);  -- 3
```

## Data Types

| SQL Type     | Python Type       | Example                    |
| ------------ | ----------------- | -------------------------- |
| BOOLEAN      | bool              | `True`                     |
| TINYINT      | int               | `127`                      |
| SMALLINT     | int               | `32767`                    |
| INT          | int               | `42`                       |
| BIGINT       | int               | `1000000`                  |
| UINT8        | int               | `255`                      |
| UINT16       | int               | `65535`                    |
| UINT32       | int               | `1000000`                  |
| UINT64       | int               | `1000000`                  |
| FLOAT        | float             | `3.14`                     |
| DOUBLE       | float             | `3.14159`                  |
| DECIMAL(P,S) | decimal.Decimal   | `Decimal("99.99")`         |
| DATE         | datetime.date     | `date(2024, 1, 15)`        |
| TIMESTAMP    | datetime.datetime | `datetime(2024, 1, 15, 10, 30)` |
| VARCHAR      | str               | `"hello"`                  |
| BINARY       | bytes             | `b"data"`                  |
| VARIANT      | any               | `{"key": "value"}`         |
| ARRAY(T)     | list[T]           | `[1, 2, 3]`                |
| MAP(K,V)     | dict[K,V]         | `{"a": 1, "b": 2}`         |
| TUPLE(T...)  | tuple(T...)       | `(1, "hello", True)`       |
| VECTOR(N)    | list[float]       | `[0.1, 0.2, 0.3]`          |

**Note:** SQL `NULL` maps to Python `None`.

## Examples

### Basic Types

```python
from databend_udf import udf
import datetime
from decimal import Decimal

@udf(input_types=["INT"], result_type="INT")
def double(x: int) -> int:
    return x * 2

@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def upper(s: str) -> str:
    return s.upper()

@udf(input_types=["DATE", "INT"], result_type="DATE")
def add_days(d: datetime.date, days: int) -> datetime.date:
    return d + datetime.timedelta(days=days)

@udf(input_types=["DECIMAL(10,2)"], result_type="DECIMAL(10,2)")
def add_tax(price: Decimal) -> Decimal:
    return price * Decimal("1.1")
```

### Complex Types

```python
from typing import List, Dict, Tuple

@udf(input_types=["ARRAY(INT)"], result_type="INT")
def array_sum(arr: List[int]) -> int:
    return sum(arr)

@udf(input_types=["MAP(VARCHAR, INT)"], result_type="INT")
def map_sum(m: Dict[str, int]) -> int:
    return sum(m.values())

@udf(input_types=["TUPLE(INT, VARCHAR)"], result_type="VARCHAR")
def tuple_format(t: Tuple[int, str]) -> str:
    return f"{t[0]}: {t[1]}"

@udf(input_types=["VECTOR(128)"], result_type="VECTOR(128)")
def normalize_vector(v: List[float]) -> List[float]:
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm > 0 else v
```

### NULL Handling

```python
from typing import Optional

# Option 1: skip_null=True - NULL input returns NULL
@udf(input_types=["INT"], result_type="INT", skip_null=True)
def double(x: int) -> int:
    return x * 2

# Option 2: skip_null=False - Handle NULL manually
@udf(input_types=["INT"], result_type="INT", skip_null=False)
def double_or_zero(x: Optional[int]) -> int:
    return x * 2 if x is not None else 0

# Nullable array elements
@udf(input_types=["ARRAY(INT NULL)"], result_type="INT", skip_null=False)
def sum_non_null(arr: List[Optional[int]]) -> int:
    return sum(x for x in arr if x is not None)
```

### Table Functions

```python
@udf(
    input_types=["INT", "INT"],
    result_type=[("left", "INT"), ("right", "INT"), ("sum", "INT")],
    batch_mode=True,
)
def expand_pairs(left: List[int], right: List[int]):
    return [
        {"left": l, "right": r, "sum": l + r}
        for l, r in zip(left, right)
    ]
```

```sql
SELECT * FROM expand_pairs([1, 2, 3], [10, 20, 30]);
```

### I/O Bound Functions

For I/O-bound functions (e.g., network requests, file operations), use `io_threads` to enable parallel processing of rows within a batch:

```python
import time

@udf(input_types=["INT"], result_type="INT", io_threads=32)
def fetch_data(id: int) -> int:
    time.sleep(0.1)  # Simulates I/O operation
    return id * 2
```

**Note:** `io_threads` controls parallelism within a single batch, not across requests.

### Concurrency Limiting

To protect your UDF server from being overwhelmed, use `max_concurrency` to limit the number of concurrent requests per function:

```python
from databend_udf import udf, UDFServer

@udf(input_types=["INT"], result_type="INT", max_concurrency=10)
def expensive_operation(x: int) -> int:
    # Only 10 concurrent requests allowed
    # Additional requests will wait for a slot
    return x * 2
```

By default, when the limit is reached, new requests **wait** until a slot becomes available. You can set a timeout to reject requests that wait too long:

```python
@udf(
    input_types=["INT"],
    result_type="INT",
    max_concurrency=10,
    concurrency_timeout=30,  # Wait up to 30 seconds, then reject
)
def expensive_operation(x: int) -> int:
    return x * 2
```

When the timeout expires, a `ConcurrencyLimitExceeded` error is raised.

You can combine `io_threads` and `max_concurrency`:

```python
@udf(
    input_types=["INT"],
    result_type="INT",
    io_threads=32,           # 32 threads for I/O within each request
    max_concurrency=5,       # But only 5 concurrent requests allowed
    concurrency_timeout=60,  # Wait up to 60s for a slot
)
def api_call(id: int) -> int:
    # Protected from too many concurrent requests
    return fetch_from_api(id)
```

## Configuration

### Databend Config

Edit `databend-query.toml`:

```toml
[query]
enable_udf_server = true
udf_server_allow_list = ["http://0.0.0.0:8815"]
```

### UDF Decorator Parameters

| Parameter             | Type               | Default | Description                                          |
| --------------------- | ------------------ | ------- | ---------------------------------------------------- |
| `input_types`         | `List[str]`        | -       | Input SQL types                                      |
| `result_type`         | `str \| List[Tuple]` | -     | Return SQL type or table schema                      |
| `name`                | `str`              | None    | Custom function name                                 |
| `skip_null`           | `bool`             | False   | Auto-return NULL for NULL inputs                     |
| `io_threads`          | `int`              | 32      | Thread pool size for parallel row processing         |
| `batch_mode`          | `bool`             | False   | Enable for table functions                           |
| `max_concurrency`     | `int`              | None    | Max concurrent requests (None = unlimited)           |
| `concurrency_timeout` | `float`            | None    | Seconds to wait for a slot (None = wait forever)     |

## Additional Resources

- [Comprehensive Examples](tests/servers/comprehensive_server.py) - All data types
- [Test Cases](tests/test_comprehensive_example.py) - Usage examples

## Development

Format code with Ruff:

```bash
pip install ruff
ruff format python/databend_udf python/tests
```

---

Inspired by [RisingWave Python API](https://pypi.org/project/risingwave/).
