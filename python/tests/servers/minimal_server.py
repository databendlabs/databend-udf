#!/usr/bin/env python3
"""
Minimal UDF server for basic connectivity tests.
Only includes built-in functions.
"""

import logging
from databend_udf import UDFServer

logging.basicConfig(level=logging.INFO)


def create_minimal_server(port=8815):
    """Create server with only built-in functions."""
    server = UDFServer(f"0.0.0.0:{port}")
    # Only built-in functions: builtin_echo, builtin_healthy
    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_minimal_server(port)
    server.serve()
