"""
Tests for StageLocation functionality.

This module tests:
- Unit tests: StageLocation parsing, injection, and validation
- Integration tests: End-to-end stage-aware UDF calls
"""

import json

import pyarrow as pa
import pytest

from databend_udf import StageLocation, UDFClient, udf
from databend_udf.udf import Headers


# =============================================================================
# Test Helpers
# =============================================================================


def _make_batch(values):
    schema = pa.schema([pa.field("value", pa.int32())])
    return pa.RecordBatch.from_arrays([pa.array(values, pa.int32())], schema=schema)


def _collect(func, batch, headers):
    results = []
    for output in func.eval_batch(batch, headers):
        results.extend(output.column(0).to_pylist())
    return results


def _s3_stage(param_name: str, bucket: str, path: str, stage_name: str = None) -> dict:
    stage_name = stage_name or param_name
    return {
        "param_name": param_name,
        "relative_path": path,
        "stage_info": {
            "stage_name": stage_name,
            "stage_type": "External",
            "stage_params": {
                "storage": {
                    "type": "s3",
                    "bucket": bucket,
                    "access_key_id": f"ak-{bucket}",
                    "secret_access_key": f"sk-{bucket}",
                }
            },
        },
    }


def _gcs_stage(param_name: str, bucket: str, path: str, stage_name: str = None) -> dict:
    stage_name = stage_name or param_name
    return {
        "param_name": param_name,
        "relative_path": path,
        "stage_info": {
            "stage_name": stage_name,
            "stage_type": "External",
            "stage_params": {
                "storage": {
                    "type": "gcs",
                    "bucket": bucket,
                    "credential": json.dumps(
                        {
                            "type": "service_account",
                            "client_email": "udf@databend.dev",
                            "private_key": "-----BEGIN PRIVATE KEY-----",
                        }
                    ),
                }
            },
        },
    }


def _azblob_stage(
    param_name: str, container: str, path: str, stage_name: str = None
) -> dict:
    stage_name = stage_name or param_name
    return {
        "param_name": param_name,
        "relative_path": path,
        "stage_info": {
            "stage_name": stage_name,
            "stage_type": "External",
            "stage_params": {
                "storage": {
                    "type": "azblob",
                    "container": container,
                    "account_name": "account",
                    "account_key": "key==",
                }
            },
        },
    }


# =============================================================================
# Test UDFs for Unit Tests
# =============================================================================


@udf(stage_refs=["stage_loc"], input_types=["INT"], result_type="VARCHAR")
def describe_stage(stage: StageLocation, value: int) -> str:
    assert stage.stage_type.lower() == "external"
    assert stage.storage
    return f"{stage.stage_name}:{stage.relative_path}:{value}"


@udf(stage_refs=["input_stage"], input_types=["INT"], result_type="INT")
def renamed_stage(input_stage: StageLocation, value: int) -> int:
    assert input_stage.storage
    return value


@udf(input_types=["INT"], result_type="INT")
def annotated_stage(stage: StageLocation, value: int) -> int:
    assert stage.storage
    return value


@udf(stage_refs=["input_stage", "output_stage"], input_types=["INT"], result_type="INT")
def multi_stage(
    input_stage: StageLocation, output_stage: StageLocation, value: int
) -> int:
    assert input_stage.storage and output_stage.storage
    return value


# =============================================================================
# Unit Tests: StageLocation Parsing and Validation
# =============================================================================


def test_stage_mapping_basic_list():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "stage_loc",
                "relative_path": "input/2024/",
                "stage_info": {
                    "stage_name": "stage_loc",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "demo"}},
                },
            }
        ]
    )
    headers = Headers({"databend-stage-mapping": [payload]})
    result = _collect(describe_stage, _make_batch([1]), headers)
    assert result == ["stage_loc:input/2024/:1"]


def test_stage_mapping_dict_payload():
    payload = json.dumps(
        {
            "stage_loc": {
                "relative_path": "path/",
                "stage_info": {
                    "stage_name": "dict_stage",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "demo"}},
                },
            }
        }
    )
    headers = Headers({"Databend-Stage-Mapping": [payload]})
    stage = headers.require_stage_locations(["stage_loc"])["stage_loc"]
    assert stage.stage_name == "dict_stage"
    assert stage.relative_path == "path/"


def test_stage_refs_rename():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "input_stage",
                "relative_path": "input/",
                "stage_info": {
                    "stage_name": "input_stage",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "alias"}},
                },
            }
        ]
    )
    headers = Headers({"Databend-Stage-Mapping": [payload]})
    assert _collect(renamed_stage, _make_batch([7]), headers) == [7]


def test_type_annotation_detection():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "stage",
                "relative_path": "annotated/",
                "stage_info": {
                    "stage_name": "annotated",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "anno"}},
                },
            }
        ]
    )
    headers = Headers({"databend-stage-mapping": [payload]})
    assert _collect(annotated_stage, _make_batch([5]), headers) == [5]


def test_multiple_stage_entries():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "input_stage",
                "relative_path": "input/",
                "stage_info": {
                    "stage_name": "input_stage",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "input"}},
                },
            },
            {
                "param_name": "output_stage",
                "relative_path": "output/",
                "stage_info": {
                    "stage_name": "output_stage",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "s3", "bucket": "output"}},
                },
            },
        ]
    )
    headers = Headers({"Databend-Stage-Mapping": [payload]})
    assert _collect(multi_stage, _make_batch([2]), headers) == [2]


def test_missing_stage_mapping():
    with pytest.raises(
        ValueError, match=r"Missing stage mapping(.|\n)*CREATE FUNCTION"
    ):
        _collect(describe_stage, _make_batch([1]), Headers())


def test_missing_storage_rejected():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "stage_loc",
                "stage_info": {
                    "stage_name": "no_storage",
                    "stage_type": "External",
                    "stage_params": {},
                },
            }
        ]
    )
    headers = Headers({"databend-stage-mapping": [payload]})
    with pytest.raises(ValueError, match="storage configuration"):
        _collect(describe_stage, _make_batch([1]), headers)


def test_internal_stage_rejected():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "stage_loc",
                "stage_info": {
                    "stage_name": "internal",
                    "stage_type": "Internal",
                    "stage_params": {"storage": {"type": "s3", "bucket": "demo"}},
                },
            }
        ]
    )
    headers = Headers({"databend-stage-mapping": [payload]})
    with pytest.raises(ValueError, match="External stage"):
        _collect(describe_stage, _make_batch([1]), headers)


def test_fs_storage_rejected():
    payload = UDFClient.format_stage_mapping(
        [
            {
                "param_name": "stage_loc",
                "stage_info": {
                    "stage_name": "bad",
                    "stage_type": "External",
                    "stage_params": {"storage": {"type": "fs", "root": "/tmp"}},
                },
            }
        ]
    )
    headers = Headers({"databend-stage-mapping": [payload]})
    with pytest.raises(ValueError, match="'fs' storage"):
        _collect(describe_stage, _make_batch([1]), headers)


# =============================================================================
# Integration Tests: End-to-end Stage-aware UDF Calls
# =============================================================================


def test_stage_integration_single_stage(stage_server):
    """Test single stage location injection via Flight."""
    client = stage_server.get_client()

    stage_locations = [_s3_stage("data_stage", "input-bucket", "data/input/")]
    result = client.call_function("stage_summary", 5, stage_locations=stage_locations)

    assert result == ["data_stage:input-bucket:data/input/:5"]


def test_stage_integration_multiple_stages(stage_server):
    """Test multiple stage locations injection via Flight."""
    client = stage_server.get_client()

    stage_locations = [
        _s3_stage("input_stage", "input-bucket", "data/input/"),
        _s3_stage("output_stage", "output-bucket", "data/output/"),
    ]

    result = client.call_function(
        "multi_stage_process", 10, stage_locations=stage_locations
    )

    expected = 10 + len("input-bucket") + len("output-bucket")
    assert result == [expected]


def test_stage_integration_gcs(stage_server):
    """Test GCS stage location."""
    client = stage_server.get_client()

    stage_locations = [
        _gcs_stage("data_stage", "gcs-bucket", "gcs/path/", stage_name="gcs_stage")
    ]
    result = client.call_function("stage_summary", 3, stage_locations=stage_locations)

    assert result == ["gcs_stage:gcs-bucket:gcs/path/:3"]


def test_stage_integration_azblob(stage_server):
    """Test Azure Blob stage location."""
    client = stage_server.get_client()

    stage_locations = [
        _azblob_stage("data_stage", "container", "azure/path/", stage_name="az_stage")
    ]
    result = client.call_function("stage_summary", 4, stage_locations=stage_locations)

    assert result == ["az_stage:container:azure/path/:4"]


def test_stage_integration_rejects_fs(stage_server):
    """Test that fs storage type is rejected."""
    client = stage_server.get_client()

    stage_locations = [
        {
            "param_name": "data_stage",
            "relative_path": "data/",
            "stage_info": {
                "stage_name": "data_stage",
                "stage_type": "External",
                "stage_params": {"storage": {"type": "fs", "root": "/tmp"}},
            },
        }
    ]

    with pytest.raises(pa.ArrowInvalid) as exc:
        client.call_function("stage_summary", 1, stage_locations=stage_locations)

    assert "'fs' storage" in str(exc.value)


def test_stage_integration_rejects_internal(stage_server):
    """Test that internal stage type is rejected."""
    client = stage_server.get_client()

    stage_locations = [
        {
            "param_name": "data_stage",
            "relative_path": "data/",
            "stage_info": {
                "stage_name": "data_stage",
                "stage_type": "Internal",
                "stage_params": {
                    "storage": {"type": "s3", "bucket": "internal-bucket"}
                },
            },
        }
    ]

    with pytest.raises(pa.ArrowInvalid) as exc:
        client.call_function("stage_summary", 1, stage_locations=stage_locations)

    assert "External stage" in str(exc.value)
