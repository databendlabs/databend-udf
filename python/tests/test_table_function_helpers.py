import pyarrow as pa

from databend_udf import Headers, udf


def _make_input_batch(func, columns):
    arrays = [
        pa.array(values, type=field.type)
        for values, field in zip(columns, func._input_schema)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=func._input_schema)


def _batch_to_rows(batch):
    data = batch.to_pydict()
    keys = list(data.keys())
    rows = []
    for idx in range(batch.num_rows):
        rows.append({key: data[key][idx] for key in keys})
    return rows


def _collect_rows(func, batch):
    rows = []
    for out_batch in func.eval_batch(batch, Headers()):
        rows.extend(_batch_to_rows(out_batch))
    return rows


@udf(
    input_types=["INT"],
    result_type=[("value", "BIGINT"), ("square", "BIGINT")],
    batch_mode=True,
)
def table_returns_record_batch(nums):
    schema = pa.schema([pa.field("value", pa.int64()), pa.field("square", pa.int64())])
    return pa.RecordBatch.from_arrays(
        [
            pa.array(nums, type=pa.int64()),
            pa.array([n * n for n in nums], type=pa.int64()),
        ],
        schema=schema,
    )


@udf(
    input_types=["INT", "INT"],
    result_type=[("left", "INT"), ("right", "INT"), ("sum", "INT")],
    batch_mode=True,
)
def table_returns_iterable(lhs, rhs):
    return [
        {"left": left_value, "right": right_value, "sum": left_value + right_value}
        for left_value, right_value in zip(lhs, rhs)
    ]


def test_table_function_accepts_record_batch():
    batch = _make_input_batch(table_returns_record_batch, [[1, 2, 3]])
    rows = _collect_rows(table_returns_record_batch, batch)
    assert rows == [
        {"value": 1, "square": 1},
        {"value": 2, "square": 4},
        {"value": 3, "square": 9},
    ]


def test_table_function_accepts_iterable_of_dicts():
    batch = _make_input_batch(table_returns_iterable, [[1, 2], [10, 20]])
    rows = _collect_rows(table_returns_iterable, batch)
    assert rows == [
        {"left": 1, "right": 10, "sum": 11},
        {"left": 2, "right": 20, "sum": 22},
    ]
