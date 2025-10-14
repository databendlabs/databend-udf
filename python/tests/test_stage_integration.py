"""End-to-end tests for stage-aware UDF functions."""

import json

import pyarrow as pa
import pytest


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


def test_stage_integration_single_stage(stage_server):
    client = stage_server.get_client()

    stage_locations = [_s3_stage("data_stage", "input-bucket", "data/input/")]
    result = client.call_function("stage_summary", 5, stage_locations=stage_locations)

    assert result == ["data_stage:input-bucket:data/input/:5"]


def test_stage_integration_multiple_stages(stage_server):
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
    client = stage_server.get_client()

    stage_locations = [
        _gcs_stage("data_stage", "gcs-bucket", "gcs/path/", stage_name="gcs_stage")
    ]
    result = client.call_function("stage_summary", 3, stage_locations=stage_locations)

    assert result == ["gcs_stage:gcs-bucket:gcs/path/:3"]


def test_stage_integration_azblob(stage_server):
    client = stage_server.get_client()

    stage_locations = [
        _azblob_stage("data_stage", "container", "azure/path/", stage_name="az_stage")
    ]
    result = client.call_function("stage_summary", 4, stage_locations=stage_locations)

    assert result == ["az_stage:container:azure/path/:4"]


def test_stage_integration_rejects_fs(stage_server):
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
