import logging
import pyarrow as pa
from prometheus_client import REGISTRY
from databend_udf import udf, StageLocation, UDFServer


@udf(input_types=["INT"], result_type="INT")
def scalar_func(x: int) -> int:
    return x


@udf(stage_refs=["stage_loc"], input_types=["INT"], result_type="INT")
def stage_func(stage_loc: StageLocation, x: int) -> int:
    return x


@udf(input_types=["INT"], result_type=["INT"], batch_mode=True)
def table_func(x: int):
    yield pa.RecordBatch.from_arrays([pa.array([x])], names=["res"])


def setup_function():
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)


def test_scalar_sql(caplog):
    with caplog.at_level(logging.INFO):
        server = UDFServer("0.0.0.0:0")
        server.add_function(scalar_func)

        assert "CREATE OR REPLACE FUNCTION scalar_func (x INT)" in caplog.text
        assert "RETURNS INT LANGUAGE python" in caplog.text


def test_stage_sql(caplog):
    with caplog.at_level(logging.INFO):
        server = UDFServer("0.0.0.0:0")
        server.add_function(stage_func)

        assert (
            "CREATE OR REPLACE FUNCTION stage_func (stage_loc STAGE_LOCATION, x INT)"
            in caplog.text
        )
        assert "RETURNS INT LANGUAGE python" in caplog.text


def test_table_sql(caplog):
    with caplog.at_level(logging.INFO):
        server = UDFServer("0.0.0.0:0")
        server.add_function(table_func)

        assert "CREATE OR REPLACE FUNCTION table_func (x INT)" in caplog.text
        assert "RETURNS TABLE (col0 INT) LANGUAGE python" in caplog.text
