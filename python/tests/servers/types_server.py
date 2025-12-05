#!/usr/bin/env python3
"""
UDF server for testing all supported data types.

This server provides UDFs that cover scalar types, complex types,
nullable types, and table functions to verify type handling.
"""

import datetime
import json
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from databend_udf import UDFServer, udf

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# Scalar Types
# ==============================================================================


@udf(input_types=["BOOLEAN"], result_type="BOOLEAN")
def negate_bool(x: bool) -> bool:
    """Boolean type test."""
    return not x


@udf(input_types=["TINYINT"], result_type="TINYINT")
def double_tinyint(x: int) -> int:
    """TINYINT: 8-bit signed integer."""
    return x * 2


@udf(input_types=["INT"], result_type="INT")
def square_int(x: int) -> int:
    """INT: 32-bit signed integer."""
    return x * x


@udf(input_types=["BIGINT"], result_type="BIGINT")
def increment_bigint(x: int) -> int:
    """BIGINT: 64-bit signed integer."""
    return x + 1


@udf(input_types=["FLOAT"], result_type="FLOAT")
def half_float(x: float) -> float:
    """FLOAT: 32-bit floating point."""
    return x / 2.0


@udf(input_types=["DOUBLE"], result_type="DOUBLE")
def double_double(x: float) -> float:
    """DOUBLE: 64-bit floating point."""
    return x * 2.0


@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def upper_varchar(s: str) -> str:
    """VARCHAR: Variable-length string."""
    return s.upper()


@udf(input_types=["BINARY"], result_type="BINARY")
def reverse_binary(b: bytes) -> bytes:
    """BINARY: Binary data."""
    return b[::-1]


@udf(input_types=["DECIMAL(10, 2)"], result_type="DECIMAL(10, 2)")
def scale_decimal(x: Decimal) -> Decimal:
    """DECIMAL: Fixed-point decimal."""
    return x * Decimal("1.5")


@udf(input_types=["DATE"], result_type="DATE")
def next_day(d: datetime.date) -> datetime.date:
    """DATE: Date without time."""
    return d + datetime.timedelta(days=1)


@udf(input_types=["TIMESTAMP"], result_type="TIMESTAMP")
def add_hour(ts: datetime.datetime) -> datetime.datetime:
    """TIMESTAMP: Date and time."""
    return ts + datetime.timedelta(hours=1)


@udf(input_types=["VARIANT"], result_type="VARIANT")
def wrap_variant(v: Any) -> Any:
    """VARIANT: JSON-like dynamic type."""
    if isinstance(v, dict):
        return {"wrapped": v}
    return {"value": v}


# ==============================================================================
# Complex Types
# ==============================================================================


@udf(input_types=["ARRAY(INT)"], result_type="ARRAY(INT)")
def double_array(arr: List[int]) -> List[int]:
    """ARRAY(INT): Array of integers."""
    return [x * 2 for x in arr]


@udf(input_types=["MAP(VARCHAR, INT)"], result_type="MAP(VARCHAR, INT)")
def double_map_values(m: Dict[str, int]) -> Dict[str, int]:
    """MAP: Key-value mapping."""
    return {k: v * 2 for k, v in m.items()}





# ==============================================================================
# Nullable Types
# ==============================================================================


@udf(input_types=["INT NULL"], result_type="INT", skip_null=True)
def nullable_double(x: Optional[int]) -> int:
    """Nullable with skip_null=True."""
    return x * 2


@udf(input_types=["INT NULL"], result_type="INT", skip_null=False)
def nullable_or_zero(x: Optional[int]) -> int:
    """Nullable with explicit handling."""
    return 0 if x is None else x * 2


# ==============================================================================
# Multi-input Functions
# ==============================================================================


@udf(input_types=["INT", "INT"], result_type="INT")
def add_ints(a: int, b: int) -> int:
    """Simple addition of two integers."""
    return a + b


@udf(input_types=["INT", "INT"], result_type="INT", skip_null=True)
def gcd(x: int, y: int) -> int:
    """Greatest common divisor."""
    while y != 0:
        x, y = y, x % y
    return x


# ==============================================================================
# Table Functions
# ==============================================================================


@udf(
    input_types=["INT", "INT"],
    result_type=[("left", "INT"), ("right", "INT"), ("sum", "INT")],
    batch_mode=True,
)
def expand_pairs(lhs: List[int], rhs: List[int]):
    """Table function returning multiple columns."""
    return [
        {"left": l, "right": r, "sum": l + r}
        for l, r in zip(lhs, rhs)
    ]


def create_server(port: int) -> UDFServer:
    """Create server with all type-testing functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    
    # Scalar types
    for fn in [
        negate_bool, double_tinyint, square_int, increment_bigint,
        half_float, double_double, upper_varchar, reverse_binary,
        scale_decimal, next_day, add_hour, wrap_variant,
    ]:
        server.add_function(fn)
    
    # Complex types
    for fn in [double_array, double_map_values]:
        server.add_function(fn)
    
    # Nullable types
    for fn in [nullable_double, nullable_or_zero]:
        server.add_function(fn)
    
    # Multi-input
    for fn in [add_ints, gcd]:
        server.add_function(fn)
    
    # Table functions
    server.add_function(expand_pairs)
    
    return server


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    create_server(port).serve()

