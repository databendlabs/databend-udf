"""
Tests for query cancellation functionality.

This module tests:
- Unit tests: QueryState, Headers context integration, eval_batch cancellation
- Integration tests: End-to-end cancellation over Flight protocol
"""

import threading
import time

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from databend_udf import Headers, udf
from databend_udf.udf import QueryCancelledError


# =============================================================================
# Test Helpers
# =============================================================================


class DummyContext:
    """Simple stand-in for pyarrow.flight.ServerCallContext."""

    def __init__(self) -> None:
        self._cancelled = False

    def is_cancelled(self) -> bool:
        return self._cancelled


def _make_input_batch(func, columns):
    arrays = [
        pa.array(values, type=field.type)
        for values, field in zip(columns, func._input_schema)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=func._input_schema)


# =============================================================================
# Unit Tests: QueryState and Headers
# =============================================================================


def test_headers_with_context_checks_cancellation():
    """Headers.query_state.is_cancelled() should check context.is_cancelled() directly."""
    ctx = DummyContext()
    headers = Headers(context=ctx)

    assert not headers.query_state.is_cancelled()

    # Simulate client disconnect/cancellation
    ctx._cancelled = True

    # is_cancelled() should immediately reflect the context state
    assert headers.query_state.is_cancelled()


def test_scalar_eval_batch_respects_cancelled_query_state():
    """Scalar UDF should raise QueryCancelledError when cancelled."""

    @udf(input_types=["INT"], result_type="INT")
    def add_one(x):
        return x + 1

    batch = _make_input_batch(add_one, [[1, 2, 3]])
    headers = Headers()
    headers.query_state.cancel()

    iterator = add_one.eval_batch(batch, headers)
    with pytest.raises(QueryCancelledError):
        next(iterator)


def test_table_eval_batch_respects_cancelled_query_state():
    """Table UDF should raise QueryCancelledError when cancelled."""

    @udf(
        input_types=["INT"],
        result_type=[("value", "INT")],
    )
    def echo_table(x):
        return [{"value": x}]

    batch = _make_input_batch(echo_table, [[1, 2]])
    headers = Headers()
    headers.query_state.cancel()

    iterator = echo_table.eval_batch(batch, headers)
    with pytest.raises(QueryCancelledError):
        next(iterator)


# =============================================================================
# Integration Tests: End-to-end Flight Cancellation
# =============================================================================


def test_client_cancellation_stops_long_running_udf(cancellation_server):
    """End-to-end test that verifies cooperative cancellation.

    The cancellation server exposes a ``cancellable_counter`` table UDF that
    sleeps ~50ms per output row and checks ``headers.query_state`` on
    each iteration. When the Flight call is cancelled, the
    ``QueryState`` is updated and the UDF stops producing rows early.
    """
    client = cancellation_server.get_client()
    flight_client: fl.FlightClient = client.client

    # The cancellable_counter(limit) UDF will, without cancellation,
    # produce roughly ``limit`` rows, each taking ~0.05s.
    limit = 100
    input_schema, batch = client._prepare_function_call(
        "cancellable_counter", args=(limit,)
    )

    descriptor = fl.FlightDescriptor.for_path("cancellable_counter")
    writer, reader = flight_client.do_exchange(descriptor)

    with writer:
        writer.begin(input_schema)
        writer.write_batch(batch)
        writer.done_writing()

    # Cancel the reader after a short delay
    def cancel_after_delay() -> None:
        time.sleep(0.3)
        reader.cancel()

    cancel_thread = threading.Thread(target=cancel_after_delay)
    cancel_thread.start()

    start = time.time()
    total_rows = 0
    try:
        for chunk in reader:
            total_rows += chunk.data.num_rows
    except Exception:
        pass

    elapsed = time.time() - start
    cancel_thread.join(timeout=5)

    # We should *not* see all rows after cooperative cancellation
    assert total_rows < limit
    # And we should finish well before the uncancelled runtime (~5s)
    assert elapsed < 3.0
