#!/usr/bin/env python3
"""
Comprehensive UDF server demonstrating all supported data types.
Shows usage examples for all input and return types.
"""

import logging
import datetime
from decimal import Decimal
from typing import List, Dict, Any, Tuple, Optional

from databend_udf import udf, UDFServer

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# 1. Scalar Types
# ==============================================================================


@udf(input_types=["BOOLEAN"], result_type="BOOLEAN")
def test_boolean(x: bool) -> bool:
    """Boolean type: True/False values"""
    return not x


@udf(input_types=["TINYINT"], result_type="TINYINT")
def test_tinyint(x: int) -> int:
    """TINYINT: 8-bit signed integer (-128 to 127)"""
    return x * 2


@udf(input_types=["SMALLINT"], result_type="SMALLINT")
def test_smallint(x: int) -> int:
    """SMALLINT: 16-bit signed integer"""
    return x + 100


@udf(input_types=["INT"], result_type="INT")
def test_int(x: int) -> int:
    """INT: 32-bit signed integer"""
    return x * x


@udf(input_types=["BIGINT"], result_type="BIGINT")
def test_bigint(x: int) -> int:
    """BIGINT: 64-bit signed integer"""
    return x + 1000000


@udf(input_types=["UINT8"], result_type="UINT8")
def test_uint8(x: int) -> int:
    """UINT8: 8-bit unsigned integer (0 to 255)"""
    return min(x + 10, 255)


@udf(input_types=["UINT16"], result_type="UINT16")
def test_uint16(x: int) -> int:
    """UINT16: 16-bit unsigned integer"""
    return x + 100


@udf(input_types=["UINT32"], result_type="UINT32")
def test_uint32(x: int) -> int:
    """UINT32: 32-bit unsigned integer"""
    return x * 2


@udf(input_types=["UINT64"], result_type="UINT64")
def test_uint64(x: int) -> int:
    """UINT64: 64-bit unsigned integer"""
    return x + 1000


@udf(input_types=["FLOAT"], result_type="FLOAT")
def test_float(x: float) -> float:
    """FLOAT: 32-bit floating point"""
    return x * 1.5


@udf(input_types=["DOUBLE"], result_type="DOUBLE")
def test_double(x: float) -> float:
    """DOUBLE: 64-bit floating point"""
    return x / 2.0


@udf(input_types=["DECIMAL(10, 2)"], result_type="DECIMAL(10, 2)")
def test_decimal(x: Decimal) -> Decimal:
    """DECIMAL: Fixed-point decimal number"""
    return x * Decimal("1.1")


@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def test_varchar(s: str) -> str:
    """VARCHAR: Variable-length string"""
    return s.upper()


@udf(input_types=["BINARY"], result_type="BINARY")
def test_binary(b: bytes) -> bytes:
    """BINARY: Binary data (bytes)"""
    return b[::-1]  # Reverse bytes


@udf(input_types=["DATE"], result_type="DATE")
def test_date(d: datetime.date) -> datetime.date:
    """DATE: Date without time"""
    return d + datetime.timedelta(days=1)


@udf(input_types=["TIMESTAMP"], result_type="TIMESTAMP")
def test_timestamp(ts: datetime.datetime) -> datetime.datetime:
    """TIMESTAMP: Date and time"""
    return ts + datetime.timedelta(hours=1)


@udf(input_types=["VARIANT"], result_type="VARIANT")
def test_variant(v: Any) -> Any:
    """VARIANT: JSON-like dynamic type"""
    if isinstance(v, dict):
        return {"wrapped": v}
    elif isinstance(v, list):
        return {"array": v, "length": len(v)}
    else:
        return {"value": v, "type": type(v).__name__}


# ==============================================================================
# 2. Complex Types
# ==============================================================================


@udf(input_types=["ARRAY(INT)"], result_type="ARRAY(INT)")
def test_array_int(arr: List[int]) -> List[int]:
    """ARRAY(INT): Array of integers"""
    return [x * 2 for x in arr]


@udf(input_types=["ARRAY(VARCHAR)"], result_type="ARRAY(VARCHAR)")
def test_array_varchar(arr: List[str]) -> List[str]:
    """ARRAY(VARCHAR): Array of strings"""
    return [s.upper() for s in arr]


@udf(input_types=["ARRAY(FLOAT)"], result_type="ARRAY(FLOAT)")
def test_array_float(arr: List[float]) -> List[float]:
    """ARRAY(FLOAT): Array of floats"""
    return [x + 0.5 for x in arr]


@udf(input_types=["ARRAY(INT NULL)"], result_type="INT")
def test_array_with_null(arr: List[Optional[int]]) -> int:
    """ARRAY with nullable elements"""
    return sum(x for x in arr if x is not None)


@udf(input_types=["MAP(VARCHAR, INT)"], result_type="MAP(VARCHAR, INT)")
def test_map(m: Dict[str, int]) -> Dict[str, int]:
    """MAP(VARCHAR, INT): Key-value mapping"""
    return {k: v * 2 for k, v in m.items()}


@udf(input_types=["MAP(VARCHAR, VARCHAR)"], result_type="VARCHAR")
def test_map_access(m: Dict[str, str]) -> str:
    """Access MAP values"""
    return m.get("key", "default")


@udf(
    input_types=["TUPLE(INT, VARCHAR, BOOLEAN)"],
    result_type="TUPLE(INT, VARCHAR, BOOLEAN)",
)
def test_tuple(t: Tuple[int, str, bool]) -> Tuple[int, str, bool]:
    """TUPLE: Fixed-size heterogeneous collection"""
    return (t[0] + 1, t[1].upper(), not t[2])


@udf(input_types=["TUPLE(INT, VARCHAR, DOUBLE)"], result_type="VARCHAR")
def test_tuple_extract(t: Tuple[int, str, float]) -> str:
    """Extract from TUPLE"""
    return f"num={t[0]}, str={t[1]}, float={t[2]:.2f}"


@udf(input_types=["VECTOR(128)"], result_type="VECTOR(128)")
def test_vector(v: List[float]) -> List[float]:
    """VECTOR: Fixed-size float array for embeddings"""
    # Normalize the vector
    magnitude = sum(x * x for x in v) ** 0.5
    if magnitude > 0:
        return [x / magnitude for x in v]
    return v


# ==============================================================================
# 3. Multiple Input Types
# ==============================================================================


@udf(input_types=["INT", "VARCHAR", "BOOLEAN"], result_type="VARCHAR")
def test_mixed_inputs(num: int, text: str, flag: bool) -> str:
    """Multiple different input types"""
    if flag:
        return f"{text}:{num}"
    else:
        return f"{num}:{text}"


@udf(input_types=["ARRAY(INT)", "ARRAY(INT)"], result_type="ARRAY(INT)")
def test_array_concat(a1: List[int], a2: List[int]) -> List[int]:
    """Concatenate two arrays"""
    return a1 + a2


@udf(input_types=["DATE", "INT"], result_type="DATE")
def test_date_arithmetic(d: datetime.date, days: int) -> datetime.date:
    """Date arithmetic: add days to date"""
    return d + datetime.timedelta(days=days)


@udf(input_types=["DECIMAL(10, 2)", "DECIMAL(10, 2)"], result_type="DECIMAL(20, 4)")
def test_decimal_multiply(a: Decimal, b: Decimal) -> Decimal:
    """Decimal multiplication with precision"""
    result = a * b
    return result.quantize(Decimal("0.0000"))


# ==============================================================================
# 4. Nullable Types
# ==============================================================================


@udf(input_types=["INT NULL"], result_type="INT", skip_null=True)
def test_nullable_skip(x: Optional[int]) -> int:
    """Nullable input with skip_null=True (NULL returns NULL automatically)"""
    return x * 2


@udf(input_types=["INT NULL"], result_type="INT", skip_null=False)
def test_nullable_handle(x: Optional[int]) -> int:
    """Nullable input with skip_null=False (handle NULL explicitly)"""
    if x is None:
        return 0
    return x * 2


@udf(
    input_types=["VARCHAR NULL", "VARCHAR"],
    result_type="VARCHAR NOT NULL",
    skip_null=False,
)
def test_nullable_string(s1: Optional[str], s2: str) -> str:
    """Mix of nullable and non-nullable inputs"""
    if s1 is None:
        return s2
    return s1 + s2


# ==============================================================================
# 5. All Types Combined
# ==============================================================================

ALL_SCALAR_TYPES = [
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIMESTAMP",
    "VARCHAR",
    "BINARY",
    "VARIANT",
]


# ==============================================================================
# 6. Table Functions
# ==============================================================================


@udf(
    input_types=["INT", "INT"],
    result_type=[
        ("id", "INT"),
        ("value", "VARCHAR"),
        ("squared", "BIGINT"),
    ],
    batch_mode=True,
)
def test_table_function(ids: List[int], multipliers: List[int]):
    """
    Table-valued function: Returns multiple columns per row

    Demonstrates:
    - Returning structured data with named columns
    - Batch mode processing
    - Multiple output columns of different types
    """
    if len(ids) != len(multipliers):
        raise ValueError("Input arrays must have same length")

    return [
        {
            "id": id_val,
            "value": f"item_{id_val}",
            "squared": id_val * id_val * mult,
        }
        for id_val, mult in zip(ids, multipliers)
    ]


def create_comprehensive_server(port=8815):
    """Create server with all type examples."""
    server = UDFServer(f"0.0.0.0:{port}")

    # Scalar types
    server.add_function(test_boolean)
    server.add_function(test_tinyint)
    server.add_function(test_smallint)
    server.add_function(test_int)
    server.add_function(test_bigint)
    server.add_function(test_uint8)
    server.add_function(test_uint16)
    server.add_function(test_uint32)
    server.add_function(test_uint64)
    server.add_function(test_float)
    server.add_function(test_double)
    server.add_function(test_decimal)
    server.add_function(test_varchar)
    server.add_function(test_binary)
    server.add_function(test_date)
    server.add_function(test_timestamp)
    server.add_function(test_variant)

    # Complex types
    server.add_function(test_array_int)
    server.add_function(test_array_varchar)
    server.add_function(test_array_float)
    server.add_function(test_array_with_null)
    server.add_function(test_map)
    server.add_function(test_map_access)
    server.add_function(test_tuple)
    server.add_function(test_tuple_extract)
    server.add_function(test_vector)

    # Mixed types
    server.add_function(test_mixed_inputs)
    server.add_function(test_array_concat)
    server.add_function(test_date_arithmetic)
    server.add_function(test_decimal_multiply)

    # Nullable types
    server.add_function(test_nullable_skip)
    server.add_function(test_nullable_handle)
    server.add_function(test_nullable_string)

    # Table functions
    server.add_function(test_table_function)

    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_comprehensive_server(port)
    server.serve()
