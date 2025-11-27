# Copyright 2023 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import inspect
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Iterator,
    Callable,
    Optional,
    Union,
    List,
    Dict,
    Any,
    Tuple,
    Mapping,
    Sequence,
    Iterable,
)
from typing import get_args, get_origin
from prometheus_client import Counter, Gauge, Histogram
from prometheus_client import start_http_server
import threading

import pyarrow as pa
from pyarrow.flight import (
    FlightServerBase,
    FlightInfo,
    ServerMiddleware,
    ServerMiddlewareFactory,
)

# comes from Databend
MAX_DECIMAL128_PRECISION = 38
MAX_DECIMAL256_PRECISION = 76
EXTENSION_KEY = b"Extension"
ARROW_EXT_TYPE_VARIANT = b"Variant"
ARROW_EXT_TYPE_VECTOR = b"Vector"

TIMESTAMP_UINT = "us"
_SCHEMA_METADATA_INPUT_COUNT_KEY = b"x-databend-udf-input-count"

logger = logging.getLogger(__name__)


class QueryState:
    """Represents the lifecycle state of a query request."""

    def __init__(self) -> None:
        self._cancelled = False
        self._start_time = time.time()

    def is_cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True
        logger.warning("Query cancelled")


class HeadersMiddleware(ServerMiddleware):
    """Flight middleware used to capture request headers for each call."""

    def __init__(self, headers) -> None:
        self.headers = headers

    def call_completed(self, exception):  # pragma: no cover - thin wrapper
        if exception:
            logger.error("Call failed", exc_info=exception)


class HeadersMiddlewareFactory(ServerMiddlewareFactory):
    """Creates `HeadersMiddleware` instances for each Flight call."""

    def start_call(self, info, headers) -> HeadersMiddleware:
        return HeadersMiddleware(headers)


def _safe_json_loads(payload: Union[str, bytes, None]) -> Optional[Any]:
    if payload is None:
        return None
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    if not isinstance(payload, str):
        return None
    payload = payload.strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        logger.debug("Failed to decode JSON payload: %s", payload)
        return None


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    decoded = _safe_json_loads(value) if isinstance(value, (str, bytes)) else None
    return decoded if isinstance(decoded, dict) else {}


def _extract_param_name(entry: Dict[str, Any]) -> Optional[str]:
    for key in ("param_name", "name", "arg_name", "stage_param", "parameter", "param"):
        value = entry.get(key)
        if value:
            return str(value)
    return None


@dataclass
class StageLocation:
    """Structured representation of a stage argument resolved by Databend."""

    name: str
    stage_name: str
    stage_type: str
    storage: Dict[str, Any]
    relative_path: str
    raw_info: Dict[str, Any]

    @classmethod
    def from_header_entry(
        cls, param_name: str, entry: Dict[str, Any]
    ) -> "StageLocation":
        entry = entry or {}
        if not isinstance(entry, dict):
            entry = {}

        stage_info = entry.get("stage_info") or entry.get("stage") or entry.get("info")
        if not isinstance(stage_info, dict):
            stage_info = _ensure_dict(stage_info)
        raw_info = stage_info if stage_info else entry

        stage_name = (
            entry.get("stage_name") or stage_info.get("stage_name")
            if isinstance(stage_info, dict)
            else None
        )
        if not stage_name:
            stage_name = entry.get("name") or param_name

        stage_type_raw = entry.get("stage_type")
        if stage_type_raw is None and isinstance(stage_info, dict):
            stage_type_raw = stage_info.get("stage_type")

        stage_type = ""
        if isinstance(stage_type_raw, str):
            stage_type = stage_type_raw
        elif isinstance(stage_type_raw, dict):
            for candidate in ("type", "stage_type"):
                candidate_value = stage_type_raw.get(candidate)
                if candidate_value:
                    stage_type = str(candidate_value)
                    break
            if not stage_type and stage_type_raw:
                first_value = next(iter(stage_type_raw.values()), None)
                if first_value:
                    stage_type = str(first_value)
        elif stage_type_raw is not None:
            stage_type = str(stage_type_raw)

        stage_params = (
            stage_info.get("stage_params") if isinstance(stage_info, dict) else {}
        )
        storage = entry.get("storage")
        if not isinstance(storage, dict):
            storage = _ensure_dict(storage)
        if not storage and isinstance(stage_params, dict):
            storage = _ensure_dict(stage_params.get("storage"))
        if not isinstance(storage, dict):
            storage = {}

        relative_path = (
            entry.get("relative_path")
            or entry.get("path")
            or entry.get("stage_path")
            or entry.get("prefix")
            or entry.get("pattern")
            or entry.get("file_path")
        )
        if isinstance(relative_path, dict):
            relative_path = relative_path.get("path") or relative_path.get("value")
        if relative_path is None:
            relative_path = ""

        return cls(
            name=str(param_name),
            stage_name=str(stage_name) if stage_name is not None else "",
            stage_type=stage_type,
            storage=storage,
            relative_path=str(relative_path),
            raw_info=raw_info if isinstance(raw_info, dict) else {},
        )


def _annotation_matches_stage_location(annotation: Any) -> bool:
    if annotation is None:
        return False
    if annotation is StageLocation:
        return True
    if isinstance(annotation, str):
        return annotation == "StageLocation"

    origin = get_origin(annotation)
    if origin is Union:
        return any(
            _annotation_matches_stage_location(arg) for arg in get_args(annotation)
        )

    return False


def _parse_stage_mapping_payload(payload: Any) -> Dict[str, StageLocation]:
    mapping: Dict[str, StageLocation] = {}

    def add_entry(param: str, value: Dict[str, Any]) -> None:
        try:
            mapping[param] = StageLocation.from_header_entry(param, value)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse stage mapping for %s: %s", param, exc)

    if isinstance(payload, list):
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            param_name = _extract_param_name(entry)
            if not param_name:
                continue
            add_entry(param_name, entry)
    elif isinstance(payload, dict):
        param_name = _extract_param_name(payload)
        if param_name:
            add_entry(param_name, payload)
        else:
            for key, value in payload.items():
                if isinstance(value, dict):
                    add_entry(str(key), value)
    return mapping


def _load_stage_mapping(header_value: Any) -> Dict[str, StageLocation]:
    """Parse the ``databend-stage-mapping`` header into StageLocation objects.

    The Flight client sends a single header whose *key* is ``databend-stage-mapping``
    (case-insensitive). The *value* is a JSON array describing every
    ``STAGE_LOCATION`` argument. For example::

        databend-stage-mapping: [
          {
            "param_name": "input_stage",
            "relative_path": "data/input/",
            "stage_info": {
              "stage_name": "input_stage",
              "stage_type": "External",
              "stage_params": {"storage": {"type": "s3", ...}}
            }
          },
          {
            "param_name": "output_stage",
            "relative_path": "data/output/",
            "stage_info": { ... }
          }
        ]

    ``stage_info`` is the JSON form of Databend's ``StageInfo`` structure and is
    forwarded verbatim so UDF handlers can access any extended metadata.
    """
    if header_value is None:
        return {}
    if isinstance(header_value, (list, tuple)):
        header_value = header_value[0] if header_value else None
    if header_value is None:
        return {}
    if isinstance(header_value, bytes):
        header_value = header_value.decode("utf-8")
    payload: Any = header_value
    if isinstance(header_value, str):
        header_value = header_value.strip()
        if not header_value:
            return {}
        try:
            payload = json.loads(header_value)
        except json.JSONDecodeError:
            logger.warning("Failed to decode Databend-Stage-Mapping header")
            return {}
    if not isinstance(payload, (list, dict)):
        return {}
    return _parse_stage_mapping_payload(payload)


class Headers:
    """Wrapper providing convenient accessors for Databend request headers."""

    def __init__(self, headers: Optional[Dict[str, Any]] = None) -> None:
        self.raw_headers: Dict[str, Any] = headers or {}
        self.query_state = QueryState()
        self._normalized: Dict[str, List[str]] = {}
        if headers:
            for key, value in headers.items():
                values: List[str] = []
                if isinstance(value, (list, tuple)):
                    iterable = value
                else:
                    iterable = [value]
                for item in iterable:
                    if isinstance(item, bytes):
                        values.append(item.decode("utf-8"))
                    else:
                        values.append(str(item) if not isinstance(item, str) else item)
                self._normalized[key.lower()] = values
        self.tenant = self._get_first("x-databend-tenant", "") or ""
        self.queryid = self._get_first("x-databend-query-id", "") or ""
        self._stage_mapping_cache: Optional[Dict[str, StageLocation]] = None

    def _get_first(self, key: str, default: Optional[str] = None) -> Optional[str]:
        values = self._normalized.get(key.lower())
        if not values:
            return default
        return values[0]

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        result = self._get_first(key, default)
        return result if result is not None else default

    def get_all(self, key: str) -> List[str]:
        return list(self._normalized.get(key.lower(), []))

    def _stage_mapping(self) -> Dict[str, StageLocation]:
        if self._stage_mapping_cache is None:
            raw_value = self.get("databend-stage-mapping")
            if raw_value is None:
                raw_value = self.get("databend_stage_mapping")
            self._stage_mapping_cache = _load_stage_mapping(raw_value)
        return self._stage_mapping_cache

    @property
    def stage_locations(self) -> Dict[str, StageLocation]:
        return dict(self._stage_mapping())

    def get_stage_location(self, name: str) -> Optional[StageLocation]:
        return self._stage_mapping().get(name)

    def require_stage_locations(self, names: List[str]) -> Dict[str, StageLocation]:
        mapping = self._stage_mapping()
        missing = [name for name in names if name not in mapping]
        if missing:
            msg = (
                "Missing stage mapping for parameter(s): "
                + ", ".join(sorted(missing))
                + ".\n"
                "Please check your CREATE FUNCTION statement to ensure that the stage location is correctly specified.\n"
                "For example: CREATE FUNCTION ... (stage_param STAGE_LOCATION) ...\n"
            )
            raise ValueError(msg)
        return {name: mapping[name] for name in names}


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """

    _name: str
    _input_schema: pa.Schema
    _result_schema: pa.Schema

    def eval_batch(
        self, batch: pa.RecordBatch, headers: Optional[Headers] = None
    ) -> Iterator[pa.RecordBatch]:
        """
        Apply the function on a batch of inputs.
        """
        return iter([])


class CallableFunction(UserDefinedFunction):
    """Shared implementation for callable UDFs.

    This handles parsing function parameters, stage bindings, and schema
    construction so both scalar and table-valued functions can reuse the logic.
    """

    _func: Callable
    _stage_ref_names: List[str]
    _headers_param: Optional[str]
    _stage_param_to_ref: Dict[str, str]
    _stage_param_set: set
    _arg_order: List[str]
    _data_arg_names: List[str]
    _call_arg_layout: List[Tuple[str, str]]
    _data_arg_indices: Dict[str, int]
    _sql_parameter_defs: List[str]
    _is_table_function: bool

    def __init__(
        self,
        func,
        input_types,
        result_fields,
        name: Optional[str] = None,
        stage_refs: Optional[List[str]] = None,
    ) -> None:
        self._func = func
        self._stage_ref_names = list(stage_refs or [])
        if len(self._stage_ref_names) != len(set(self._stage_ref_names)):
            raise ValueError("stage_refs contains duplicate parameter names")

        spec = inspect.getfullargspec(func)
        arg_names = list(spec.args)
        self._headers_param = None
        if arg_names and arg_names[-1] == "headers":
            self._headers_param = arg_names.pop()

        annotations = getattr(spec, "annotations", {})
        annotated_stage_params = [
            name
            for name in arg_names
            if _annotation_matches_stage_location(annotations.get(name))
        ]

        if self._stage_ref_names:
            if all(ref in arg_names for ref in self._stage_ref_names):
                stage_param_python_names = list(self._stage_ref_names)
            else:
                stage_param_python_names = annotated_stage_params
            if len(stage_param_python_names) != len(self._stage_ref_names):
                raise ValueError(
                    f"Unable to map stage_refs to function parameters for {func.__name__}"
                )
        else:
            stage_param_python_names = annotated_stage_params
            self._stage_ref_names = list(stage_param_python_names)

        if self._stage_ref_names and not stage_param_python_names:
            raise ValueError(
                f"stage_refs specified for function {func.__name__} but no StageLocation parameters found"
            )

        if len(stage_param_python_names) != len(set(stage_param_python_names)):
            raise ValueError("Stage parameters must be unique in function signature")

        self._stage_param_to_ref = {
            param: ref
            for param, ref in zip(stage_param_python_names, self._stage_ref_names)
        }
        self._stage_param_set = set(stage_param_python_names)

        self._arg_order = list(arg_names)
        self._data_arg_names = [
            name for name in self._arg_order if name not in self._stage_param_set
        ]
        input_type_list = _to_list(input_types)
        if len(self._data_arg_names) != len(input_type_list):
            raise ValueError(
                f"Function {func.__name__} expects {len(self._data_arg_names)} data argument(s) "
                f"but {len(input_type_list)} input type(s) were provided"
            )

        data_fields = [
            _to_arrow_field(type_def).with_name(arg_name)
            for arg_name, type_def in zip(self._data_arg_names, input_type_list)
        ]
        self._input_schema = pa.schema(data_fields)
        self._data_arg_indices = {
            name: idx for idx, name in enumerate(self._data_arg_names)
        }

        self._result_schema = pa.schema(result_fields)
        self._name = name or (
            func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        )

        self._call_arg_layout = []
        for parameter in self._arg_order:
            if parameter in self._stage_param_set:
                self._call_arg_layout.append(("stage", parameter))
            else:
                self._call_arg_layout.append(("data", parameter))
        if self._headers_param:
            self._call_arg_layout.append(("headers", self._headers_param))

        data_field_map = {field.name: field for field in self._input_schema}
        self._sql_parameter_defs = []
        for kind, identifier in self._call_arg_layout:
            if kind == "stage":
                stage_ref_name = self._stage_param_to_ref.get(identifier, identifier)
                self._sql_parameter_defs.append(f"{stage_ref_name} STAGE_LOCATION")
            elif kind == "data":
                field = data_field_map[identifier]
                self._sql_parameter_defs.append(
                    f"{field.name} {_arrow_field_to_string(field)}"
                )

        self._is_table_function = False

    def _ensure_headers(self, headers: Optional[Headers]) -> Headers:
        return headers if isinstance(headers, Headers) else Headers(headers)

    def _resolve_stage_locations(
        self, headers_obj: Headers
    ) -> Dict[str, StageLocation]:
        stage_locations: Dict[str, StageLocation] = {}
        if self._stage_ref_names:
            stage_locations_by_ref = headers_obj.require_stage_locations(
                self._stage_ref_names
            )
            stage_locations = {
                param: stage_locations_by_ref[ref]
                for param, ref in self._stage_param_to_ref.items()
            }
            for name, location in stage_locations.items():
                stage_type = location.stage_type.lower() if location.stage_type else ""
                if stage_type and stage_type != "external":
                    raise ValueError(
                        f"Stage parameter '{name}' must reference an External stage"
                    )
                if not location.storage:
                    raise ValueError(
                        f"Stage parameter '{name}' is missing storage configuration"
                    )
                storage_type = str(location.storage.get("type", "")).lower()
                if storage_type == "fs":
                    raise ValueError(
                        f"Stage parameter '{name}' must not use 'fs' storage"
                    )
        return stage_locations

    def _convert_inputs(self, batch: pa.RecordBatch) -> List[List[Any]]:
        processed_inputs: List[List[Any]] = []
        for array, field in zip(batch, self._input_schema):
            python_values = [value.as_py() for value in array]
            processed_inputs.append(
                _input_process_func(_list_field(field))(python_values)
            )
        return processed_inputs

    def _assemble_args(
        self,
        data_inputs: List[List[Any]],
        stage_locations: Dict[str, StageLocation],
        headers: Headers,
        row_idx: Optional[int],
    ) -> List[Any]:
        args: List[Any] = []
        for kind, identifier in self._call_arg_layout:
            if kind == "data":
                data_index = self._data_arg_indices[identifier]
                values = data_inputs[data_index]
                args.append(values if row_idx is None else values[row_idx])
            elif kind == "stage":
                if identifier not in stage_locations:
                    raise ValueError(
                        f"Missing stage mapping for parameter '{identifier}'"
                    )
                args.append(stage_locations[identifier])
            elif kind == "headers":
                args.append(headers)
        return args

    def _row_has_null(self, data_inputs: List[List[Any]], row_idx: int) -> bool:
        for values in data_inputs:
            if values[row_idx] is None:
                return True
        return False

    def __call__(self, *args):
        return self._func(*args)


class ScalarFunction(CallableFunction):
    """User-defined scalar function implementation."""

    _io_threads: Optional[int]
    _executor: Optional[ThreadPoolExecutor]
    _skip_null: bool
    _batch_mode: bool

    def __init__(
        self,
        func,
        input_types,
        result_type,
        name=None,
        stage_refs: Optional[List[str]] = None,
        io_threads=None,
        skip_null=None,
        batch_mode=False,
    ):
        result_fields, is_table = _normalize_result_type(result_type)
        if is_table:
            raise ValueError("ScalarFunction result_type must describe a single value")

        super().__init__(func, input_types, result_fields, name, stage_refs)
        self._io_threads = io_threads
        self._batch_mode = batch_mode
        self._executor = (
            ThreadPoolExecutor(max_workers=self._io_threads)
            if self._io_threads is not None
            else None
        )

        if skip_null and not self._result_schema.field(0).nullable:
            raise ValueError(
                f"Return type of function {self._name} must be nullable when skip_null is True"
            )

        self._skip_null = skip_null or False
        self._is_table_function = False

    def eval_batch(
        self, batch: pa.RecordBatch, headers: Optional[Headers] = None
    ) -> Iterator[pa.RecordBatch]:
        headers_obj = self._ensure_headers(headers)
        stage_locations = self._resolve_stage_locations(headers_obj)
        processed_inputs = self._convert_inputs(batch)

        if self._batch_mode:
            call_args = self._assemble_args(
                processed_inputs, stage_locations, headers_obj, row_idx=None
            )
            column = self._func(*call_args)
        elif self._executor is not None:
            row_count = batch.num_rows
            column = [None] * row_count
            futures = []
            future_rows: List[int] = []
            for row in range(row_count):
                if self._skip_null and self._row_has_null(processed_inputs, row):
                    column[row] = None
                    continue
                call_args = self._assemble_args(
                    processed_inputs, stage_locations, headers_obj, row
                )
                futures.append(self._executor.submit(self._func, *call_args))
                future_rows.append(row)
            for row, future in zip(future_rows, futures):
                column[row] = future.result()
        else:
            column = []
            for row in range(batch.num_rows):
                if self._skip_null and self._row_has_null(processed_inputs, row):
                    column.append(None)
                    continue
                call_args = self._assemble_args(
                    processed_inputs, stage_locations, headers_obj, row
                )
                column.append(self._func(*call_args))

        column = _output_process_func(_list_field(self._result_schema.field(0)))(column)

        array = pa.array(column, type=self._result_schema.types[0])
        yield pa.RecordBatch.from_arrays([array], schema=self._result_schema)


class TableFunction(CallableFunction):
    """Table-valued function that returns zero or more rows."""

    _batch_mode: bool

    def __init__(
        self,
        func,
        input_types,
        result_type,
        name=None,
        stage_refs: Optional[List[str]] = None,
        io_threads=None,
        skip_null=None,
        batch_mode=False,
    ):
        if io_threads not in (None, 0, 1):
            raise ValueError("Table functions do not support io_threads > 1")
        if skip_null:
            raise ValueError("Table functions do not support skip_null semantics")

        result_fields, is_table = _normalize_result_type(result_type)
        if not is_table:
            raise ValueError(
                "TableFunction result_type must describe a table result (list of columns)"
            )

        super().__init__(func, input_types, result_fields, name, stage_refs)
        self._batch_mode = bool(batch_mode)
        self._is_table_function = True

    def eval_batch(
        self, batch: pa.RecordBatch, headers: Optional[Headers] = None
    ) -> Iterator[pa.RecordBatch]:
        headers_obj = self._ensure_headers(headers)
        stage_locations = self._resolve_stage_locations(headers_obj)
        processed_inputs = self._convert_inputs(batch)

        if self._batch_mode:
            call_args = self._assemble_args(
                processed_inputs, stage_locations, headers_obj, row_idx=None
            )
            yield from self._iter_output_batches(self._func(*call_args))
        else:
            for row in range(batch.num_rows):
                call_args = self._assemble_args(
                    processed_inputs, stage_locations, headers_obj, row
                )
                yield from self._iter_output_batches(self._func(*call_args))

    def _iter_output_batches(self, result: Any) -> Iterator[pa.RecordBatch]:
        if result is None:
            return
        if isinstance(result, pa.RecordBatch):
            yield self._align_record_batch(result)
            return
        if isinstance(result, pa.Table):
            for batch in result.to_batches():
                yield self._align_record_batch(batch)
            return
        if isinstance(result, Mapping):
            if self._is_column_mapping(result):
                yield self._dict_to_batch(result)
            else:
                yield self._build_batch_from_rows([result])
            return
        if isinstance(result, Iterable) and not isinstance(result, (bytes, str)):
            pending_rows: List[Any] = []
            for item in result:
                if isinstance(item, (pa.RecordBatch, pa.Table)):
                    if pending_rows:
                        yield self._build_batch_from_rows(pending_rows)
                        pending_rows = []
                    yield from self._iter_output_batches(item)
                elif isinstance(item, Mapping) and self._is_column_mapping(item):
                    if pending_rows:
                        yield self._build_batch_from_rows(pending_rows)
                        pending_rows = []
                    yield self._dict_to_batch(item)
                else:
                    pending_rows.append(item)
            if pending_rows:
                yield self._build_batch_from_rows(pending_rows)
            return
        raise TypeError(
            f"Table function {self._name} must yield records, mappings, or pyarrow batches"
        )

    def _dict_to_batch(self, mapping: Mapping[str, Sequence[Any]]) -> pa.RecordBatch:
        lengths = set()
        columns: List[pa.Array] = []
        for field in self._result_schema:
            if field.name not in mapping:
                raise ValueError(
                    f"Missing expected column '{field.name}' in column mapping"
                )
            values = list(mapping[field.name])
            lengths.add(len(values))
            columns.append(self._column_from_values(field, values))
        if len(lengths) > 1:
            raise ValueError("Column mapping contains columns of differing lengths")
        return pa.RecordBatch.from_arrays(columns, schema=self._result_schema)

    def _build_batch_from_rows(self, rows: List[Any]) -> pa.RecordBatch:
        if not rows:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in self._result_schema],
                schema=self._result_schema,
            )
        first = rows[0]
        if isinstance(first, Mapping) and not self._is_column_mapping(first):
            return self._to_batch_from_row_dicts(rows)
        if isinstance(first, Sequence) and not isinstance(first, (bytes, str)):
            return self._to_batch_from_row_sequences(rows)
        raise TypeError(
            f"Table function {self._name} returned unsupported rows of type {type(first)}"
        )

    def _to_batch_from_row_dicts(self, rows: List[Mapping[str, Any]]) -> pa.RecordBatch:
        columns: List[pa.Array] = []
        for field in self._result_schema:
            values = []
            for row in rows:
                if not isinstance(row, Mapping):
                    raise TypeError("Row dictionaries must be Mapping instances")
                if field.name not in row:
                    raise ValueError(
                        f"Missing column '{field.name}' in table function row"
                    )
                values.append(row[field.name])
            columns.append(self._column_from_values(field, values))
        return pa.RecordBatch.from_arrays(columns, schema=self._result_schema)

    def _to_batch_from_row_sequences(self, rows: List[Sequence[Any]]) -> pa.RecordBatch:
        if not rows:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in self._result_schema],
                schema=self._result_schema,
            )
        column_count = len(self._result_schema)
        columns: List[pa.Array] = []
        for idx, field in enumerate(self._result_schema):
            values = []
            for row in rows:
                if not isinstance(row, Sequence) or isinstance(row, (bytes, str)):
                    raise TypeError("Row sequences must be Sequence instances")
                if len(row) != column_count:
                    raise ValueError(
                        f"Row sequence length {len(row)} does not match expected column count {column_count}"
                    )
                values.append(row[idx])
            columns.append(self._column_from_values(field, values))
        return pa.RecordBatch.from_arrays(columns, schema=self._result_schema)

    def _column_from_values(self, field: pa.Field, values: Sequence[Any]) -> pa.Array:
        processor = _output_process_func(field)
        processed = [processor(value) for value in values]
        return pa.array(processed, type=field.type)

    def _align_record_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if batch.schema.equals(self._result_schema, check_metadata=False):
            return batch
        columns = []
        for field in self._result_schema:
            index = batch.schema.get_field_index(field.name)
            if index == -1:
                raise ValueError(
                    f"Missing expected column '{field.name}' in table function result"
                )
            source_field = batch.schema.field(index)
            if not source_field.type.equals(field.type, check_metadata=False):
                raise ValueError(
                    f"Column '{field.name}' type mismatch: expected {field.type}, got {source_field.type}"
                )
            columns.append(batch.column(index))
        return pa.RecordBatch.from_arrays(columns, schema=self._result_schema)

    def _is_column_mapping(self, mapping: Mapping[str, Any]) -> bool:
        if not mapping:
            return True
        return all(
            isinstance(values, Sequence) and not isinstance(values, (bytes, str))
            for values in mapping.values()
        )


def _normalize_result_type(
    result_type: Any, default_name: str = "output"
) -> Tuple[List[pa.Field], bool]:
    """Normalize the declared result_type into Arrow fields.

    Returns:
        (fields, is_table) where ``is_table`` signals if the result describes
        multiple columns.
    """
    if isinstance(result_type, pa.Schema):
        fields = list(result_type)
        if not fields:
            raise ValueError("Result schema must contain at least one column")
        _ensure_unique_names([field.name for field in fields])
        return fields, True

    if isinstance(result_type, pa.Field):
        field = result_type if result_type.name else result_type.with_name(default_name)
        return [field], True

    if isinstance(result_type, (list, tuple)) and not isinstance(
        result_type, (str, bytes)
    ):
        fields: List[pa.Field] = []
        for idx, item in enumerate(result_type):
            if isinstance(item, pa.Field):
                field = item if item.name else item.with_name(f"col{idx}")
            elif isinstance(item, tuple) and len(item) == 2:
                name, type_def = item
                field = _to_arrow_field(type_def).with_name(str(name))
            else:
                field = _to_arrow_field(item).with_name(f"col{idx}")
            fields.append(field)
        if not fields:
            raise ValueError(
                "Table function result_type must contain at least one column"
            )
        _ensure_unique_names([field.name for field in fields])
        return fields, True

    field = _to_arrow_field(result_type).with_name(default_name)
    return [field], False


def _ensure_unique_names(names: List[str]) -> None:
    if len(names) != len(set(names)):
        raise ValueError("Result columns must have unique names")


def udf(
    input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
    result_type: Union[str, pa.DataType],
    name: Optional[str] = None,
    stage_refs: Optional[List[str]] = None,
    io_threads: Optional[int] = 32,
    skip_null: Optional[bool] = False,
    batch_mode: Optional[bool] = False,
) -> Callable:
    """Annotation for defining scalar or table-valued UDFs.

    Parameters:
    - input_types: describe each argument's type.
    - result_type: single type or column list describing the result(s).
    - name: optional function name exposed to Databend.
    - stage_refs: optional list of parameter names treated as stage locations.
    - io_threads: number of threads for IO bound scalar functions.
    - skip_null: when True, NULL inputs bypass the function and yield NULL output.
    - batch_mode: allow passing entire columns to the function rather than per-row calls.
    """

    is_table = isinstance(result_type, (pa.Schema, pa.Field)) or (
        isinstance(result_type, Sequence) and not isinstance(result_type, (str, bytes))
    )

    if is_table:

        def decorator(f):
            return TableFunction(
                f,
                input_types,
                result_type,
                name,
                stage_refs=stage_refs,
                io_threads=None,
                skip_null=skip_null,
                batch_mode=batch_mode,
            )

        return decorator

    if io_threads is not None and io_threads > 1:

        def decorator(f):
            return ScalarFunction(
                f,
                input_types,
                result_type,
                name,
                stage_refs=stage_refs,
                io_threads=io_threads,
                skip_null=skip_null,
                batch_mode=batch_mode,
            )

        return decorator

    def decorator(f):
        return ScalarFunction(
            f,
            input_types,
            result_type,
            name,
            stage_refs=stage_refs,
            skip_null=skip_null,
            batch_mode=batch_mode,
        )

    return decorator


class UDFServer(FlightServerBase):
    """
    A server that provides user-defined functions to clients.

    Example:
    ```
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(my_udf)
    server.serve()
    ```
    """

    _location: str
    _functions: Dict[str, UserDefinedFunction]

    def __init__(self, location="0.0.0.0:8815", metric_location=None, **kwargs):
        middleware = dict(kwargs.pop("middleware", {}))
        middleware.setdefault("log_headers", HeadersMiddlewareFactory())
        super(UDFServer, self).__init__(
            "grpc://" + location, middleware=middleware, **kwargs
        )
        self._location = location
        self._metric_location = metric_location
        self._functions = {}

        # Initialize Prometheus metrics
        self.requests_count = Counter(
            "udf_server_requests_count",
            "Total number of UDF requests processed",
            ["function_name"],
        )
        self.rows_count = Counter(
            "udf_server_rows_count", "Total number of rows processed", ["function_name"]
        )
        self.running_requests = Gauge(
            "udf_server_running_requests_count",
            "Number of currently running UDF requests",
            ["function_name"],
        )
        self.running_rows = Gauge(
            "udf_server_running_rows_count",
            "Number of currently processing rows",
            ["function_name"],
        )
        self.response_duration = Histogram(
            "udf_server_response_duration_seconds",
            "Time spent processing UDF requests",
            ["function_name"],
            buckets=(
                0.005,
                0.01,
                0.025,
                0.05,
                0.075,
                0.1,
                0.25,
                0.5,
                0.75,
                1.0,
                2.5,
                5.0,
                7.5,
                10.0,
            ),
        )

        self.error_count = Counter(
            "udf_server_errors_count",
            "Total number of UDF processing errors",
            ["function_name", "error_type"],
        )

        self.add_function(builtin_echo)
        self.add_function(builtin_healthy)

    def _start_metrics_server(self):
        """Start Prometheus metrics HTTP server if metric_location is provided"""
        try:
            host, port = self._metric_location.split(":")
            port = int(port)

            def start_server():
                start_http_server(port, host)
                logger.info(
                    f"Prometheus metrics server started on {self._metric_location}"
                )

            metrics_thread = threading.Thread(target=start_server, daemon=True)
            metrics_thread.start()
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            raise

    def get_flight_info(self, context, descriptor):
        """Return the result schema of a function."""
        func_name = descriptor.path[0].decode("utf-8")
        if func_name not in self._functions:
            raise ValueError(f"Function {func_name} does not exists")
        udf = self._functions[func_name]
        # return the concatenation of input and output schema
        full_schema = pa.schema(list(udf._input_schema) + list(udf._result_schema))
        metadata = dict(full_schema.metadata.items()) if full_schema.metadata else {}
        metadata[_SCHEMA_METADATA_INPUT_COUNT_KEY] = str(len(udf._input_schema)).encode(
            "utf-8"
        )
        full_schema = full_schema.with_metadata(metadata)
        return FlightInfo(
            schema=full_schema,
            descriptor=descriptor,
            endpoints=[],
            total_records=len(full_schema),
            total_bytes=0,
        )

    def do_exchange(self, context, descriptor, reader, writer):
        """Call a function from the client."""
        func_name = descriptor.path[0].decode("utf-8")
        if func_name not in self._functions:
            raise ValueError(f"Function {func_name} does not exists")
        udf = self._functions[func_name]
        writer.begin(udf._result_schema)

        headers_middleware = context.get_middleware("log_headers")
        request_headers = Headers(
            headers_middleware.headers if headers_middleware else None
        )

        # Increment request counter
        self.requests_count.labels(function_name=func_name).inc()
        # Increment running requests gauge
        self.running_requests.labels(function_name=func_name).inc()

        try:
            with self.response_duration.labels(function_name=func_name).time():
                for batch in reader:
                    # Update row metrics
                    batch_rows = batch.data.num_rows
                    self.rows_count.labels(function_name=func_name).inc(batch_rows)
                    self.running_rows.labels(function_name=func_name).inc(batch_rows)

                    try:
                        for output_batch in udf.eval_batch(batch.data, request_headers):
                            writer.write_batch(output_batch)
                    finally:
                        # Decrease running rows gauge after processing
                        self.running_rows.labels(function_name=func_name).dec(
                            batch_rows
                        )

        except Exception as e:
            self.error_count.labels(
                function_name=func_name, error_type=e.__class__.__name__
            ).inc()
            logger.exception(e)
            raise e
        finally:
            # Decrease running requests gauge
            self.running_requests.labels(function_name=func_name).dec()

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf._name
        if name in self._functions:
            raise ValueError("Function already exists: " + name)
        self._functions[name] = udf
        parameter_defs = getattr(udf, "_sql_parameter_defs", None)
        if parameter_defs is None:
            parameter_defs = [
                f"{field.name} {_arrow_field_to_string(field)}"
                for field in udf._input_schema
            ]
        input_types = ", ".join(parameter_defs)
        if getattr(udf, "_is_table_function", False):
            column_defs = ", ".join(
                f"{field.name} {_inner_field_to_string(field)}"
                for field in udf._result_schema
            )
            output_type = f"TABLE ({column_defs})"
        else:
            output_type = _arrow_field_to_string(udf._result_schema[0])
        sql = (
            f"CREATE OR REPLACE FUNCTION {name} ({input_types}) "
            f"RETURNS {output_type} LANGUAGE python "
            f"HANDLER = '{name}' ADDRESS = 'http://{self._location}';"
        )
        logger.info(f"added function: {name}, SQL:\n{sql}\n")

    def serve(self):
        """Start the server."""
        logger.info(f"UDF server listening on {self._location}")
        if self._metric_location:
            self._start_metrics_server()
            logger.info(
                f"Prometheus metrics available at http://{self._metric_location}/metrics"
            )

        super(UDFServer, self).serve()


def _input_process_func(field: pa.Field) -> Callable:
    """
    Return a function to process input value.

    - Tuple=pa.struct(): dict -> tuple
    - Json=pa.large_binary(): bytes -> Any
    - Map=pa.map_(): list[tuple(k,v)] -> dict
    """
    if pa.types.is_list(field.type) or pa.types.is_fixed_size_list(field.type):
        func = _input_process_func(field.type.value_field)
        return (
            lambda array: [func(v) if v is not None else None for v in array]
            if array is not None
            else None
        )
    if pa.types.is_struct(field.type):
        funcs = [_input_process_func(f) for f in field.type]
        # the input value of struct type is a dict
        # we convert it into tuple here
        return (
            lambda map: tuple(
                func(v) if v is not None else None
                for v, func in zip(map.values(), funcs)
            )
            if map is not None
            else None
        )
    if pa.types.is_map(field.type):
        funcs = [
            _input_process_func(field.type.key_field),
            _input_process_func(field.type.item_field),
        ]
        # list[tuple[k,v]] -> dict
        return (
            lambda array: dict(
                tuple(func(v) for v, func in zip(item, funcs)) for item in array
            )
            if array is not None
            else None
        )
    if pa.types.is_large_binary(field.type):
        if _field_is_variant(field):
            return lambda v: json.loads(v) if v is not None else None

    return lambda v: v


def _output_process_func(field: pa.Field) -> Callable:
    """
    Return a function to process output value.

    - Json=pa.large_binary(): Any -> str
    - Map=pa.map_(): dict -> list[tuple(k,v)]
    """
    if pa.types.is_list(field.type) or pa.types.is_fixed_size_list(field.type):
        func = _output_process_func(field.type.value_field)
        return (
            lambda array: [func(v) if v is not None else None for v in array]
            if array is not None
            else None
        )
    if pa.types.is_struct(field.type):
        funcs = [_output_process_func(f) for f in field.type]
        return (
            lambda tup: tuple(
                func(v) if v is not None else None for v, func in zip(tup, funcs)
            )
            if tup is not None
            else None
        )
    if pa.types.is_map(field.type):
        funcs = [
            _output_process_func(field.type.key_field),
            _output_process_func(field.type.item_field),
        ]
        # dict -> list[tuple[k,v]]
        return (
            lambda map: [
                tuple(func(v) for v, func in zip(item, funcs)) for item in map.items()
            ]
            if map is not None
            else None
        )
    if pa.types.is_large_binary(field.type):
        if _field_is_variant(field):
            return lambda v: json.dumps(_ensure_str(v)) if v is not None else None

    return lambda v: v


def _null_func(*args):
    return None


def _list_field(field: pa.Field) -> pa.Field:
    return pa.field("", pa.list_(field))


def _to_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


def _ensure_str(x):
    if isinstance(x, bytes):
        return x.decode("utf-8")
    elif isinstance(x, list):
        return [_ensure_str(v) for v in x]
    elif isinstance(x, dict):
        return {_ensure_str(k): _ensure_str(v) for k, v in x.items()}
    else:
        return x


def _field_is_variant(field: pa.Field) -> bool:
    if field.metadata is None:
        return False
    if field.metadata.get(EXTENSION_KEY) == ARROW_EXT_TYPE_VARIANT:
        return True
    return False


def _to_arrow_field(t: Union[str, pa.DataType]) -> pa.Field:
    """
    Convert a string or pyarrow.DataType to pyarrow.Field.
    """
    if isinstance(t, str):
        return _type_str_to_arrow_field(t)
    else:
        return pa.field("", t, False)


def _type_str_to_arrow_field(type_str: str) -> pa.Field:
    """
    Convert a SQL data type to `pyarrow.Field`.
    """
    type_str = type_str.strip().upper()
    nullable = True
    if type_str.endswith("NULL"):
        type_str = type_str[:-4].strip()
        if type_str.endswith("NOT"):
            type_str = type_str[:-3].strip()
            nullable = False

    return _type_str_to_arrow_field_inner(type_str).with_nullable(nullable)


def _type_str_to_arrow_field_inner(type_str: str) -> pa.Field:
    type_str = type_str.strip().upper()
    if type_str in ("BOOLEAN", "BOOL"):
        return pa.field("", pa.bool_(), False)
    elif type_str in ("TINYINT", "INT8"):
        return pa.field("", pa.int8(), False)
    elif type_str in ("SMALLINT", "INT16"):
        return pa.field("", pa.int16(), False)
    elif type_str in ("INT", "INTEGER", "INT32"):
        return pa.field("", pa.int32(), False)
    elif type_str in ("BIGINT", "INT64"):
        return pa.field("", pa.int64(), False)
    elif type_str in ("TINYINT UNSIGNED", "UINT8"):
        return pa.field("", pa.uint8(), False)
    elif type_str in ("SMALLINT UNSIGNED", "UINT16"):
        return pa.field("", pa.uint16(), False)
    elif type_str in ("INT UNSIGNED", "INTEGER UNSIGNED", "UINT32"):
        return pa.field("", pa.uint32(), False)
    elif type_str in ("BIGINT UNSIGNED", "UINT64"):
        return pa.field("", pa.uint64(), False)
    elif type_str in ("FLOAT", "FLOAT32"):
        return pa.field("", pa.float32(), False)
    elif type_str in ("FLOAT64", "DOUBLE"):
        return pa.field("", pa.float64(), False)
    elif type_str == "DATE":
        return pa.field("", pa.date32(), False)
    elif type_str in ("DATETIME", "TIMESTAMP"):
        return pa.field("", pa.timestamp(TIMESTAMP_UINT), False)
    elif type_str in ("STRING", "VARCHAR", "CHAR", "CHARACTER", "TEXT"):
        return pa.field("", pa.large_utf8(), False)
    elif type_str in ("BINARY"):
        return pa.field("", pa.large_binary(), False)
    elif type_str in ("VARIANT", "JSON"):
        # In Databend, JSON type is identified by the "EXTENSION" key in the metadata.
        return pa.field(
            "",
            pa.large_binary(),
            nullable=False,
            metadata={EXTENSION_KEY: ARROW_EXT_TYPE_VARIANT},
        )
    elif type_str.startswith("NULLABLE"):
        type_str = type_str[8:].strip("()").strip()
        return _type_str_to_arrow_field_inner(type_str).with_nullable(True)
    elif type_str.endswith("NULL"):
        type_str = type_str[:-4].strip()
        return _type_str_to_arrow_field_inner(type_str).with_nullable(True)
    elif type_str.startswith("DECIMAL"):
        # DECIMAL(precision, scale)
        str_list = type_str[7:].strip("()").split(",")
        precision = int(str_list[0].strip())
        scale = int(str_list[1].strip())
        if precision < 1 or precision > MAX_DECIMAL256_PRECISION:
            raise ValueError(
                f"Decimal precision must be between 1 and {MAX_DECIMAL256_PRECISION}"
            )
        elif scale > precision:
            raise ValueError(
                f"Decimal scale must be between 0 and precision {precision}"
            )

        if precision < MAX_DECIMAL128_PRECISION:
            return pa.field("", pa.decimal128(precision, scale), False)
        else:
            return pa.field("", pa.decimal256(precision, scale), False)
    elif type_str.startswith("ARRAY"):
        # ARRAY(INT)
        type_str = type_str[5:].strip("()").strip()
        return pa.field("", pa.list_(_type_str_to_arrow_field_inner(type_str)), False)
    elif type_str.startswith("MAP"):
        # MAP(STRING, INT)
        str_list = type_str[3:].strip("()").split(",")
        key_field = _type_str_to_arrow_field_inner(str_list[0].strip())
        val_field = _type_str_to_arrow_field_inner(str_list[1].strip())
        return pa.field("", pa.map_(key_field, val_field), False)
    elif type_str.startswith("TUPLE"):
        # TUPLE(STRING, INT, INT)
        str_list = type_str[5:].strip("()").split(",")
        fields = []
        for type_str in str_list:
            type_str = type_str.strip()
            fields.append(_type_str_to_arrow_field_inner(type_str))
        return pa.field("", pa.struct(fields), False)
    elif type_str.startswith("VECTOR"):
        # VECTOR(1024)
        dim = int(type_str[6:].strip("()").strip())
        # Use List(float) with metadata to represent VECTOR(N)
        # This is a workaround because Databend UDF client might not support FixedSizeList yet.
        return pa.field(
            "",
            pa.list_(pa.field("item", pa.float32(), nullable=False)),
            nullable=False,
            metadata={
                EXTENSION_KEY: ARROW_EXT_TYPE_VECTOR,
                b"vector_size": str(dim).encode(),
            },
        )
    else:
        raise ValueError(f"Unsupported type: {type_str}")


def _arrow_field_to_string(field: pa.Field) -> str:
    """
    Convert a `pyarrow.Field` to a SQL data type string.
    """
    type_str = _field_type_to_string(field)
    return f"{type_str} NOT NULL" if not field.nullable else type_str


def _inner_field_to_string(field: pa.Field) -> str:
    # inner field default is NOT NULL in databend
    type_str = _field_type_to_string(field)
    return f"{type_str} NOT NULL" if not field.nullable else type_str


def _field_type_to_string(field: pa.Field) -> str:
    """
    Convert a `pyarrow.DataType` to a SQL data type string.
    """
    t = field.type
    if field.metadata and field.metadata.get(EXTENSION_KEY) == ARROW_EXT_TYPE_VECTOR:
        dim = int(field.metadata.get(b"vector_size", b"0"))
        return f"VECTOR({dim})"

    if pa.types.is_boolean(t):
        return "BOOLEAN"
    elif pa.types.is_int8(t):
        return "TINYINT"
    elif pa.types.is_int16(t):
        return "SMALLINT"
    elif pa.types.is_int32(t):
        return "INT"
    elif pa.types.is_int64(t):
        return "BIGINT"
    elif pa.types.is_uint8(t):
        return "TINYINT UNSIGNED"
    elif pa.types.is_uint16(t):
        return "SMALLINT UNSIGNED"
    elif pa.types.is_uint32(t):
        return "INT UNSIGNED"
    elif pa.types.is_uint64(t):
        return "BIGINT UNSIGNED"
    elif pa.types.is_float32(t):
        return "FLOAT"
    elif pa.types.is_float64(t):
        return "DOUBLE"
    elif pa.types.is_decimal(t):
        return f"DECIMAL({t.precision}, {t.scale})"
    elif pa.types.is_date32(t):
        return "DATE"
    elif pa.types.is_timestamp(t):
        return "TIMESTAMP"
    elif pa.types.is_large_unicode(t) or pa.types.is_unicode(t):
        return "VARCHAR"
    elif pa.types.is_large_binary(t) or pa.types.is_binary(t):
        if _field_is_variant(field):
            return "VARIANT"
        else:
            return "BINARY"
    elif pa.types.is_list(t):
        return f"ARRAY({_inner_field_to_string(t.value_field)})"
    elif pa.types.is_map(t):
        return f"MAP({_inner_field_to_string(t.key_field)}, {_inner_field_to_string(t.item_field)})"
    elif pa.types.is_struct(t):
        args_str = ", ".join(_inner_field_to_string(field) for field in t)
        return f"TUPLE({args_str})"
    else:
        raise ValueError(f"Unsupported type: {t}")


@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def builtin_echo(a):
    return a


@udf(input_types=[], result_type="INT")
def builtin_healthy():
    return 1
