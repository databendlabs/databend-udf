"""
Pytest configuration and fixtures for UDF tests.
Provides isolated server instances for each test.
"""

import pytest
import subprocess
import sys
import time
import os
import socket
from databend_udf import UDFClient


def find_free_port():
    """Find a free port for server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


class ServerManager:
    """Manages UDF server lifecycle for testing."""

    def __init__(self, server_script, port=None):
        self.server_script = server_script
        self.port = port or find_free_port()
        self.process = None

    def start(self):
        """Start the server."""
        env = os.environ.copy()
        env["UDF_SERVER_PORT"] = str(self.port)
        # Add python directory to Python path for imports
        python_dir = os.path.dirname(os.path.dirname(__file__))
        env["PYTHONPATH"] = python_dir

        self.process = subprocess.Popen(
            [sys.executable, self.server_script, str(self.port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # Check if process started
        time.sleep(0.5)  # Give server more time to start
        if self.process.poll() is not None:
            stdout, stderr = self.process.communicate()
            print(f"Server failed to start. Exit code: {self.process.returncode}")
            print(f"Stderr: {stderr}")
            return False

        # Wait for server to be ready
        client = UDFClient(port=self.port)
        max_attempts = 30

        for attempt in range(max_attempts):
            try:
                # Test direct function call instead of health_check
                result = client.call_function("builtin_healthy")
                if result and len(result) > 0 and result[0] == 1:
                    print(f"Health check successful on attempt {attempt + 1}")
                    return True
                else:
                    print(f"Health check returned {result} on attempt {attempt + 1}")

            except Exception as e:
                if attempt < 3:  # Show first few errors
                    print(f"Attempt {attempt + 1}, Error: {e}")
                    print(f"Error type: {type(e)}")
                pass
            time.sleep(0.1)

        # Failed to connect, show debug info
        print(f"Health check failed after {max_attempts} attempts")
        if self.process.poll() is None:
            print("Server process is still running")
        else:
            print(f"Server process exited with code: {self.process.returncode}")
            stdout, stderr = self.process.communicate()
            print(f"Server stdout: {stdout}")
            print(f"Server stderr: {stderr}")

        self.stop()
        return False

    def stop(self):
        """Stop the server."""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

    def get_client(self):
        """Get client for this server."""
        return UDFClient(port=self.port)


def _create_fixture(script_name: str, fixture_name: str):
    """Helper to create a server fixture."""
    script_path = os.path.join(os.path.dirname(__file__), "servers", script_name)
    manager = ServerManager(script_path)

    if not manager.start():
        pytest.fail(f"Failed to start {fixture_name}")

    return manager


@pytest.fixture
def minimal_server():
    """Minimal server with only built-in functions.

    Use for: Basic connectivity tests, health checks.
    """
    manager = _create_fixture("minimal_server.py", "minimal server")
    yield manager
    manager.stop()


@pytest.fixture
def types_server():
    """Server with comprehensive type-testing functions.

    Use for: Testing all supported data types (scalar, complex, nullable).
    """
    manager = _create_fixture("types_server.py", "types server")
    yield manager
    manager.stop()


@pytest.fixture
def stage_server():
    """Server with stage-aware UDFs.

    Use for: Testing StageLocation parameter handling.
    """
    manager = _create_fixture("stage_server.py", "stage server")
    yield manager
    manager.stop()


@pytest.fixture
def concurrency_server():
    """Server with concurrency-limited functions.

    Use for: Testing max_concurrency and concurrency_timeout.
    """
    manager = _create_fixture("concurrency_server.py", "concurrency server")
    yield manager
    manager.stop()


@pytest.fixture
def cancellation_server():
    """Server with cancellation-aware functions.

    Use for: Testing client disconnect/cancellation handling.
    """
    manager = _create_fixture("cancellation_server.py", "cancellation server")
    yield manager
    manager.stop()
