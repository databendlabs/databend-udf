"""
Simple client library for testing Databend UDF servers.
"""

import pyarrow as pa
import pyarrow.flight as fl
from typing import List, Any


class UDFClient:
    """Simple client for calling UDF functions on a Databend UDF server."""

    def __init__(self, host: str = "localhost", port: int = 8815):
        """
        Initialize UDF client.

        Args:
            host: Server host (default: localhost)
            port: Server port (default: 8815)
        """
        self.location = f"grpc://{host}:{port}"
        self.client = fl.FlightClient(self.location)
        self._schema_cache = {}

    def _get_cached_schema(self, function_name: str) -> pa.Schema:
        """Get function schema with lightweight caching."""
        if function_name not in self._schema_cache:
            info = self.get_function_info(function_name)
            self._schema_cache[function_name] = info.schema
        return self._schema_cache[function_name]

    def _prepare_function_call(
        self, function_name: str, args: tuple = (), kwargs: dict = None
    ) -> tuple:
        """
        Centralized helper to prepare schema and batch for function calls.

        Returns:
            tuple: (input_schema, batch)
        """
        kwargs = kwargs or {}
        schema = self._get_cached_schema(function_name)

        # Validate arguments
        if args and kwargs:
            raise ValueError("Cannot mix positional and keyword arguments")

        if args:
            # Positional arguments - validate count first
            total_fields = len(schema)
            expected_input_count = total_fields - 1  # Last field is always output

            if len(args) != expected_input_count:
                raise ValueError(
                    f"Function '{function_name}' expects {expected_input_count} arguments, got {len(args)}"
                )

            if len(args) == 0:
                input_schema = pa.schema([])
                # For zero-argument functions, create a batch with 1 row and no columns
                dummy_array = pa.array([None])
                temp_batch = pa.RecordBatch.from_arrays(
                    [dummy_array], schema=pa.schema([pa.field("dummy", pa.null())])
                )
                batch = temp_batch.select([])
            else:
                input_fields = [schema.field(i) for i in range(len(args))]
                input_schema = pa.schema(input_fields)

                arrays = []
                for i, arg in enumerate(args):
                    if isinstance(arg, list):
                        arrays.append(pa.array(arg, type=input_schema.field(i).type))
                    else:
                        arrays.append(pa.array([arg], type=input_schema.field(i).type))
                batch = pa.RecordBatch.from_arrays(arrays, schema=input_schema)
        else:
            # Keyword arguments
            if len(kwargs) == 0:
                input_schema = pa.schema([])
                # Create empty batch with one row
                dummy_array = pa.array([None])
                temp_batch = pa.RecordBatch.from_arrays(
                    [dummy_array], schema=pa.schema([pa.field("dummy", pa.null())])
                )
                batch = temp_batch.select([])
            else:
                # Extract only input fields (exclude the last field which is output)
                # The schema contains input fields + 1 output field
                total_fields = len(schema)
                num_input_fields = total_fields - 1  # Last field is always output
                input_fields = [schema.field(i) for i in range(num_input_fields)]
                input_schema = pa.schema(input_fields)

                # Validate kwargs
                expected_fields = {field.name for field in input_schema}
                provided_fields = set(kwargs.keys())

                missing = expected_fields - provided_fields
                extra = provided_fields - expected_fields

                if missing:
                    raise ValueError(f"Missing required arguments: {missing}")
                if extra:
                    raise ValueError(f"Unexpected arguments: {extra}")

                # Validate array lengths are consistent
                if kwargs:
                    array_lengths = [
                        len(v) if isinstance(v, list) else 1 for v in kwargs.values()
                    ]
                    if len(set(array_lengths)) > 1:
                        raise ValueError(
                            f"All batch arrays must have the same length, got lengths: {array_lengths}"
                        )

                # Create arrays in schema order
                arrays = []
                for field in input_schema:
                    arrays.append(pa.array(kwargs[field.name], type=field.type))
                batch = pa.RecordBatch.from_arrays(arrays, schema=input_schema)

        return input_schema, batch

    def get_function_info(self, function_name: str) -> fl.FlightInfo:
        """
        Get function schema information.

        Args:
            function_name: Name of the UDF function

        Returns:
            FlightInfo containing input and output schema
        """
        descriptor = fl.FlightDescriptor.for_path(function_name)
        return self.client.get_flight_info(descriptor)

    def call_function(self, function_name: str, *args) -> List[Any]:
        """
        Call a UDF function with given arguments.

        Args:
            function_name: Name of the UDF function
            *args: Function arguments

        Returns:
            List of result values
        """
        input_schema, batch = self._prepare_function_call(function_name, args)

        # Call function
        descriptor = fl.FlightDescriptor.for_path(function_name)
        writer, reader = self.client.do_exchange(descriptor=descriptor)

        with writer:
            writer.begin(input_schema)
            writer.write_batch(batch)
            writer.done_writing()

            # Get results
            results = []
            for result_chunk in reader:
                result_batch = result_chunk.data
                for i in range(result_batch.num_rows):
                    results.append(result_batch.column(0)[i].as_py())

        return results

    def call_function_batch(self, function_name: str, **kwargs) -> List[Any]:
        """
        Call a UDF function with batch data.

        Args:
            function_name: Name of the UDF function
            **kwargs: Named arguments with list values

        Returns:
            List of result values
        """
        input_schema, batch = self._prepare_function_call(function_name, kwargs=kwargs)

        # Call function
        descriptor = fl.FlightDescriptor.for_path(function_name)
        writer, reader = self.client.do_exchange(descriptor=descriptor)

        with writer:
            writer.begin(input_schema)
            writer.write_batch(batch)
            writer.done_writing()

            # Get results
            results = []
            for result_chunk in reader:
                result_batch = result_chunk.data
                for i in range(result_batch.num_rows):
                    results.append(result_batch.column(0)[i].as_py())

        return results

    def list_functions(self) -> List[str]:
        """
        List available functions by testing common function names.

        Returns:
            List of available function names
        """
        # Test built-in functions
        functions = []
        test_functions = ["builtin_echo", "builtin_healthy"]

        for func_name in test_functions:
            try:
                self.get_function_info(func_name)
                functions.append(func_name)
            except Exception:
                pass

        return functions

    def health_check(self) -> bool:
        """
        Check if server is healthy.

        Returns:
            True if server is healthy
        """
        try:
            result = self.call_function("builtin_healthy")
            return result == [1]
        except Exception:
            return False

    def echo(self, message: str) -> str:
        """
        Echo a message (useful for testing connectivity).

        Args:
            message: Message to echo

        Returns:
            Echoed message
        """
        result = self.call_function("builtin_echo", message)
        return result[0] if result else None


def create_client(host: str = "localhost", port: int = 8815) -> UDFClient:
    """
    Create a UDF client instance.

    Args:
        host: Server host (default: localhost)
        port: Server port (default: 8815)

    Returns:
        UDFClient instance
    """
    return UDFClient(host, port)
