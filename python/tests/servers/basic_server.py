#!/usr/bin/env python3
"""
Basic UDF server with simple arithmetic functions.
"""

import logging
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


def create_basic_server(port=8815):
    """Create server with basic arithmetic functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    server.add_function(add_two_ints)
    server.add_function(gcd)
    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_basic_server(port)
    server.serve()
