#!/usr/bin/env python3
"""
UDF server for testing cancellation/interruption behavior.

This server provides UDFs that are intentionally slow and check for
cancellation state, allowing tests to verify that client disconnection
or cancellation is properly propagated to the UDF execution.
"""

import logging
import time

from databend_udf import UDFServer, udf

logging.basicConfig(level=logging.INFO)


@udf(input_types=["INT"], result_type="INT")
def slow_scalar(value: int) -> int:
    """A slow scalar function that takes 50ms per call."""
    time.sleep(0.05)
    return value


@udf(
    input_types=["INT"],
    result_type=[("value", "INT")],
    io_threads=None,
)
def cancellable_counter(limit: int, headers=None):
    """A slow table UDF that cooperatively checks for cancellation.

    This function produces rows slowly (50ms each) and checks
    `headers.query_state.is_cancelled()` on each iteration.
    When the client cancels, it stops producing rows early.

    Used by integration tests to verify that Flight's cancellation
    state is correctly propagated into the per-request Headers.
    """
    rows = []
    for i in range(limit):
        if headers is not None and headers.query_state.is_cancelled():
            break
        time.sleep(0.05)
        rows.append({"value": i})
    return rows


@udf(
    input_types=["INT"],
    result_type=[("index", "INT"), ("data", "VARCHAR")],
    io_threads=None,
)
def cancellable_generator(count: int, headers=None):
    """Another cancellable table function for testing.

    Generates rows with index and data, checking for cancellation.
    """
    rows = []
    for i in range(count):
        if headers is not None and headers.query_state.is_cancelled():
            break
        time.sleep(0.03)
        rows.append({"index": i, "data": f"row_{i}"})
    return rows


def create_server(port: int) -> UDFServer:
    """Create server with cancellation-testable functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    server.add_function(slow_scalar)
    server.add_function(cancellable_counter)
    server.add_function(cancellable_generator)
    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    create_server(port).serve()

