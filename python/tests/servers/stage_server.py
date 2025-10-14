#!/usr/bin/env python3
"""Stage-aware UDF server used in integration tests."""

import logging
import sys

from databend_udf import StageLocation, UDFServer, udf

logging.basicConfig(level=logging.INFO)


@udf(stage_refs=["data_stage"], input_types=["INT"], result_type="VARCHAR")
def stage_summary(stage: StageLocation, value: int) -> str:
    assert stage.stage_type.lower() == "external"
    assert stage.storage
    bucket = stage.storage.get("bucket", stage.storage.get("container", ""))
    return f"{stage.stage_name}:{bucket}:{stage.relative_path}:{value}"


@udf(
    stage_refs=["input_stage", "output_stage"],
    input_types=["INT"],
    result_type="INT",
)
def multi_stage_process(  # Align with documentation name
    input_stage: StageLocation, output_stage: StageLocation, value: int
) -> int:
    assert input_stage.storage and output_stage.storage
    assert input_stage.stage_type.lower() == "external"
    assert output_stage.stage_type.lower() == "external"
    # Simple deterministic behaviour for testing
    return (
        value
        + len(input_stage.storage.get("bucket", ""))
        + len(output_stage.storage.get("bucket", ""))
    )


def create_server(port: int):
    server = UDFServer(f"0.0.0.0:{port}")
    server.add_function(stage_summary)
    server.add_function(multi_stage_process)
    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    server = create_server(port)
    server.serve()
