#!/usr/bin/env python3
"""
Basic UDF server with simple arithmetic functions.
"""

import logging
import os
import sys

import pyarrow as pa

try:
    from databend_udf import udf, UDFServer
except ModuleNotFoundError:  # pragma: no cover - fallback for local execution
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from databend_udf import udf, UDFServer

logging.basicConfig(level=logging.INFO)


@udf(input_types=["INT", "INT"], result_type="INT")
def add_two_ints(a, b):
    return a + b


@udf(input_types=["INT", "INT"], result_type="INT", skip_null=True)
def gcd(x, y):
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(
    input_types=["INT"],
    result_type=[("num", "INT"), ("double_num", "INT")],
    batch_mode=True,
)
def expand_numbers(nums):
    doubled = [value * 2 for value in nums]
    schema = pa.schema(
        [
            pa.field("num", pa.int32(), nullable=False),
            pa.field("double_num", pa.int32(), nullable=False),
        ]
    )
    return pa.RecordBatch.from_arrays(
        [
            pa.array(nums, type=pa.int32()),
            pa.array(doubled, type=pa.int32()),
        ],
        schema=schema,
    )


def create_basic_server(port=8815):
    """Create server with basic arithmetic functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    server.add_function(add_two_ints)
    server.add_function(gcd)
    server.add_function(expand_numbers)
    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_basic_server(port)
    server.serve()
