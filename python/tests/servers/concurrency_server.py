#!/usr/bin/env python3
"""
UDF server with concurrency-limited functions for testing max_concurrency.
"""

import logging
import time
from databend_udf import udf, UDFServer

logging.basicConfig(level=logging.INFO)


# max_concurrency=2 with timeout=0.1s - will timeout quickly if waiting
@udf(input_types=["INT"], result_type="INT", max_concurrency=2, concurrency_timeout=0.1)
def slow_add_limited(x):
    """A slow function with max_concurrency=2 and short timeout for testing."""
    time.sleep(0.5)
    return x + 1


@udf(input_types=["INT"], result_type="INT")
def slow_add_unlimited(x):
    """A slow function without concurrency limit."""
    time.sleep(0.3)
    return x + 1


# max_concurrency=1 with timeout=0.1s
@udf(input_types=["INT"], result_type="INT", max_concurrency=1, concurrency_timeout=0.1)
def single_concurrency(x):
    """A function that only allows 1 concurrent request."""
    time.sleep(0.5)
    return x * 2


# Table function with max_concurrency=2 and timeout
@udf(
    input_types=["INT"],
    result_type=[("value", "INT")],
    max_concurrency=2,
    concurrency_timeout=0.1,
)
def table_func_limited(x):
    """A table function with max_concurrency=2."""
    time.sleep(0.3)
    return [{"value": x + i} for i in range(3)]


# max_concurrency=2 with no timeout - will wait indefinitely
@udf(input_types=["INT"], result_type="INT", max_concurrency=2)
def slow_add_wait(x):
    """A function with max_concurrency=2 that waits indefinitely."""
    time.sleep(0.3)
    return x + 1


def create_concurrency_server(port=8815):
    """Create server with concurrency-limited functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    server.add_function(slow_add_limited)
    server.add_function(slow_add_unlimited)
    server.add_function(single_concurrency)
    server.add_function(table_func_limited)
    server.add_function(slow_add_wait)
    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_concurrency_server(port)
    server.serve()
