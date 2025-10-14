"""Standalone demo server for integration tests."""

import datetime
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from databend_udf import UDFServer, udf


@udf(input_types=["INT", "INT", "INT", "INT"], result_type="INT")
def add_four(a: int, b: int, c: int, d: int) -> int:
    return a + b + c + d


@udf(input_types=["BOOLEAN", "INT", "INT"], result_type="INT")
def select_if(flag: bool, lhs: int, rhs: int) -> int:
    return lhs if flag else rhs


@udf(input_types=["VARCHAR", "VARCHAR", "VARCHAR"], result_type="VARCHAR")
def split_and_join(text: str, delimiter: str, glue: str) -> str:
    return glue.join(text.split(delimiter))


@udf(input_types=["BINARY"], result_type="BINARY")
def reverse_bytes(payload: bytes) -> bytes:
    return payload[::-1]


@udf(input_types=["DECIMAL(36, 18)", "DECIMAL(36, 18)"], result_type="DECIMAL(72, 28)")
def decimal_div(lhs: Decimal, rhs: Decimal) -> Decimal:
    result = lhs / rhs
    return result.quantize(Decimal("0." + "0" * 28))


@udf(input_types=["DATE", "INT"], result_type="DATE")
def add_days(base: datetime.date, days: int) -> datetime.date:
    return base + datetime.timedelta(days=days)


@udf(input_types=["ARRAY(VARCHAR)", "INT"], result_type="VARCHAR")
def array_access(values: List[str], index: int) -> Optional[str]:
    if index <= 0 or index > len(values):
        return None
    return values[index - 1]


@udf(input_types=["MAP(VARCHAR,VARCHAR)", "VARCHAR"], result_type="VARCHAR")
def map_lookup(mapping: Dict[str, str], key: str) -> Optional[str]:
    return mapping.get(key)


@udf(input_types=["INT"], result_type="INT")
def wait(value: int) -> int:
    time.sleep(0.05)
    return value


@udf(
    input_types=["ARRAY(VARCHAR)", "INT", "INT"],
    result_type="ARRAY(VARCHAR)",
)
def tuple_slice(values: List[Any], i: int, j: int) -> List[Any]:
    first = values[i] if 0 <= i < len(values) else None
    second = values[j] if 0 <= j < len(values) else None
    return [first, second]


@udf(
    input_types=[
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIMESTAMP",
        "VARCHAR",
        "VARIANT",
    ],
    result_type="VARIANT",
)
def all_types_snapshot(
    flag,
    tiny,
    small,
    integer,
    big,
    flt,
    dbl,
    date,
    timestamp,
    text,
    variant,
):
    if isinstance(variant, str):
        try:
            variant_value = json.loads(variant)
        except json.JSONDecodeError:
            variant_value = variant
    else:
        variant_value = variant
    return {
        "flag": flag,
        "tiny": tiny,
        "small": small,
        "integer": integer,
        "big": big,
        "flt": flt,
        "dbl": dbl,
        "date": date.isoformat() if date else None,
        "timestamp": timestamp.isoformat() if timestamp else None,
        "text": text,
        "variant": variant_value,
    }


def create_server(port: int) -> UDFServer:
    server = UDFServer(f"0.0.0.0:{port}")
    for func in [
        add_four,
        select_if,
        split_and_join,
        reverse_bytes,
        decimal_div,
        add_days,
        array_access,
        map_lookup,
        wait,
        tuple_slice,
        all_types_snapshot,
    ]:
        server.add_function(func)
    return server


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    create_server(port).serve()
