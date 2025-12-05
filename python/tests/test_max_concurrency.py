"""
Tests for max_concurrency feature.
Validates that UDF functions can limit concurrent requests.
"""

import time
import threading


def test_concurrency_limit_exceeded(concurrency_server):
    """Test that requests are rejected when max_concurrency is exceeded."""
    client = concurrency_server.get_client()
    results = []
    errors = []
    started = threading.Event()

    def call_slow_func():
        started.wait()  # Wait for all threads to be ready
        try:
            result = client.call_function("slow_add_limited", 1)
            results.append(result)
        except Exception as e:
            # Capture errors for later inspection
            errors.append(e)

    # Start 4 concurrent requests, but max_concurrency=2
    threads = []
    for _ in range(4):
        t = threading.Thread(target=call_slow_func)
        threads.append(t)
        t.start()

    # Signal all threads to start at once
    time.sleep(0.1)  # Give threads time to reach wait()
    started.set()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # With max_concurrency=2 and 4 concurrent requests,
    # some should succeed and some should fail
    assert len(results) >= 1, f"At least one request should succeed, errors={errors}"
    assert len(errors) >= 1, f"At least one request should be rejected, errors={errors}"

    # Check that errors are concurrency limit errors
    for e in errors:
        assert (
            "concurrency limit" in str(e).lower()
            or "ConcurrencyLimitExceeded" in str(type(e).__name__)
            or "concurrency" in str(e).lower()
        )


def test_single_concurrency(concurrency_server):
    """Test function with max_concurrency=1."""
    client = concurrency_server.get_client()
    results = []
    errors = []
    started = threading.Event()

    def call_single():
        started.wait()  # Wait for all threads to be ready
        try:
            result = client.call_function("single_concurrency", 5)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Start 3 concurrent requests with max_concurrency=1
    threads = []
    for _ in range(3):
        t = threading.Thread(target=call_single)
        threads.append(t)
        t.start()

    # Signal all threads to start at once
    time.sleep(0.1)
    started.set()

    for t in threads:
        t.join()

    # At least one should succeed, others should fail
    assert len(results) >= 1, f"At least one request should succeed, errors={errors}"
    assert len(errors) >= 1, f"At least one request should be rejected, errors={errors}"


def test_unlimited_concurrency(concurrency_server):
    """Test that function without max_concurrency allows all requests."""
    client = concurrency_server.get_client()
    results = []
    errors = []

    def call_unlimited():
        try:
            result = client.call_function("slow_add_unlimited", 1)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Start 4 concurrent requests - all should succeed
    threads = []
    for _ in range(4):
        t = threading.Thread(target=call_unlimited)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    # All should succeed since there's no limit
    assert len(results) == 4
    assert len(errors) == 0


def test_sequential_requests_succeed(concurrency_server):
    """Test that sequential requests all succeed even with limit."""
    client = concurrency_server.get_client()

    # Sequential calls should all succeed
    for i in range(5):
        result = client.call_function("slow_add_limited", i)
        assert result == [i + 1]


def test_concurrency_limit_releases_after_completion(concurrency_server):
    """Test that semaphore is released after request completes."""
    client = concurrency_server.get_client()

    # First batch: fill up the limit
    result1 = client.call_function("slow_add_limited", 1)
    assert result1 == [2]

    # After completion, should be able to make another request
    result2 = client.call_function("slow_add_limited", 2)
    assert result2 == [3]


def test_table_function_with_concurrency_limit(concurrency_server):
    """Test that table functions also respect max_concurrency."""
    client = concurrency_server.get_client()
    results = []
    errors = []
    started = threading.Event()

    def call_table_func():
        started.wait()
        try:
            result = client.call_function("table_func_limited", 10)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Start 4 concurrent requests with max_concurrency=2
    threads = []
    for _ in range(4):
        t = threading.Thread(target=call_table_func)
        threads.append(t)
        t.start()

    time.sleep(0.1)
    started.set()

    for t in threads:
        t.join()

    # Some should succeed, some should fail
    assert len(results) >= 1, f"At least one request should succeed, errors={errors}"
    assert len(errors) >= 1, f"At least one request should be rejected, errors={errors}"


def test_concurrency_wait_mode(concurrency_server):
    """Test that requests wait when no timeout is set (wait indefinitely)."""
    client = concurrency_server.get_client()
    results = []
    errors = []
    started = threading.Event()

    def call_wait_func():
        started.wait()
        try:
            result = client.call_function("slow_add_wait", 1)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Start 4 concurrent requests with max_concurrency=2 but no timeout
    # All should eventually succeed because they wait
    threads = []
    for _ in range(4):
        t = threading.Thread(target=call_wait_func)
        threads.append(t)
        t.start()

    time.sleep(0.1)
    started.set()

    for t in threads:
        t.join()

    # All requests should succeed because they wait for a slot
    assert (
        len(results) == 4
    ), f"All 4 requests should succeed, got {len(results)}, errors={errors}"
    assert len(errors) == 0, f"No errors expected, got {len(errors)}, errors={errors}"
