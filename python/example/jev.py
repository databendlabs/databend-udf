# Copyright 2025 Databend Labs
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

"""Jev integration for the Databend Python UDF server.

* jev(row, condition) -> boolean
* jev_prob(row, condition) -> probability in [0, 1]
* jev_choice(row, question, options) -> the most likely option
* jev_score(row, question, levels) -> probability-weighted level position
* jev_eval(row, question, kind, options) -> the complete Jev answer

Rows are sent to TypeSafe's System One API in batches. Set TYPESAFE_API_KEY
before starting this server; see the README at the bottom of this file for
other optional environment variables.
"""

import json
import logging
import os
import random
import threading
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from databend_udf import UDFServer, udf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = os.getenv("JEV_API_URL", "https://api.typesafe.ai/v1/systemone")
MODEL = os.getenv("JEV_MODEL", "jev-latest")
BATCH_SIZE = max(1, int(os.getenv("JEV_BATCH_SIZE", "20")))
CONCURRENCY = max(1, int(os.getenv("JEV_CONCURRENCY", "6")))
TIMEOUT = max(0.1, float(os.getenv("JEV_TIMEOUT", "30")))
MAX_RETRIES = max(0, int(os.getenv("JEV_MAX_RETRIES", "3")))
CACHE_SIZE = max(0, int(os.getenv("JEV_CACHE_SIZE", "10000")))
DEFAULT_THRESHOLD = float(os.getenv("JEV_THRESHOLD", "0.5"))

_EXECUTOR = ThreadPoolExecutor(max_workers=CONCURRENCY, thread_name_prefix="jev")
_CACHE: "OrderedDict[Tuple[str, str, str, str, Tuple[str, ...]], Dict[str, Any]]" = (
    OrderedDict()
)
_CACHE_LOCK = threading.Lock()


class JevError(RuntimeError):
    """Raised when a Jev request is invalid or the API call fails."""


def _api_key() -> str:
    key = os.getenv("TYPESAFE_API_KEY")
    if not key:
        raise JevError(
            "TYPESAFE_API_KEY is not set; create a key at "
            "https://console.typesafe.ai and export it before starting the server"
        )
    return key


def _canonical_json(value: Any) -> str:
    """Return stable JSON used both for the request and the result cache key."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _cache_get(key: Tuple[str, str, str, str, Tuple[str, ...]]):
    if CACHE_SIZE == 0:
        return None
    with _CACHE_LOCK:
        answer = _CACHE.get(key)
        if answer is not None:
            _CACHE.move_to_end(key)
        return answer


def _cache_put(
    key: Tuple[str, str, str, str, Tuple[str, ...]], answer: Dict[str, Any]
) -> None:
    if CACHE_SIZE == 0:
        return
    with _CACHE_LOCK:
        _CACHE[key] = answer
        _CACHE.move_to_end(key)
        while len(_CACHE) > CACHE_SIZE:
            _CACHE.popitem(last=False)


def _question(kind: str, index: int, query: str, options: Tuple[str, ...]):
    row_ref = "rows[%d]" % index
    if kind == "noul":
        return {
            "type": "noul",
            "instructions": (
                "Does the record `%s` satisfy the condition stated in `condition`?"
                % row_ref
            ),
        }
    if kind == "score":
        return {
            "type": "score",
            "instructions": "Rate the record `%s`: %s" % (row_ref, query),
            "criteria": list(options),
        }
    if kind == "choice":
        return {
            "type": "choice",
            "instructions": "For the record `%s`: %s" % (row_ref, query),
            "criteria": {option: None for option in options},
        }
    raise JevError("kind must be one of: noul, score, choice")


def _retry_delay(error: HTTPError, attempt: int) -> float:
    retry_after_ms = error.headers.get("retry-after-ms")
    if retry_after_ms and retry_after_ms.isdigit():
        return min(float(retry_after_ms) / 1000.0, 30.0)

    retry_after = error.headers.get("retry-after")
    if retry_after:
        try:
            return min(float(retry_after), 30.0)
        except ValueError:
            pass

    return min(0.5 * (2**attempt), 8.0) + random.random() * 0.25


def _call_api(
    rows: Sequence[Any], kind: str, query: str, options: Tuple[str, ...]
) -> List[Dict[str, Any]]:
    state: Dict[str, Any] = {"rows": rows}
    if kind == "noul":
        state["condition"] = query

    payload = json.dumps(
        {
            "model": MODEL,
            "state": state,
            "questions": {
                "r%d" % i: _question(kind, i, query, options) for i in range(len(rows))
            },
        },
        ensure_ascii=False,
    ).encode("utf-8")

    headers = {
        "Authorization": "Bearer " + _api_key(),
        "Content-Type": "application/json",
        "User-Agent": "databend-udf-jev/1.0",
    }

    for attempt in range(MAX_RETRIES + 1):
        request = Request(API_URL, data=payload, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=TIMEOUT) as response:
                response_data = json.loads(response.read().decode("utf-8"))
            answers = response_data.get("answers") or {}
            missing = ["r%d" % i for i in range(len(rows)) if "r%d" % i not in answers]
            if missing:
                raise JevError(
                    "Jev API response is missing answers: %s" % ", ".join(missing)
                )
            return [answers["r%d" % i] for i in range(len(rows))]
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")[:500]
            retryable = error.code in (408, 429, 529) or error.code >= 500
            if not retryable or attempt == MAX_RETRIES:
                raise JevError(
                    "Jev API returned HTTP %d: %s" % (error.code, body)
                ) from error
            time.sleep(_retry_delay(error, attempt))
        except (URLError, TimeoutError, OSError) as error:
            if attempt == MAX_RETRIES:
                raise JevError(
                    "Jev API is unreachable after %d attempt(s): %s"
                    % (MAX_RETRIES + 1, error)
                ) from error
            time.sleep(min(0.5 * (2**attempt), 8.0) + random.random() * 0.25)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise JevError("Jev API returned invalid JSON: %s" % error) from error

    raise AssertionError("unreachable")


def _normalise_options(kind: str, options: Optional[Sequence[str]]) -> Tuple[str, ...]:
    if kind == "noul":
        return ()
    if not options:
        raise JevError("%s requires at least one option" % kind)
    result = tuple(str(option) for option in options)
    if len(set(result)) != len(result):
        raise JevError("%s options must be unique" % kind)
    return result


def _evaluate(
    rows: Sequence[Any],
    queries: Sequence[Optional[str]],
    kinds: Sequence[str],
    option_columns: Optional[Sequence[Optional[Sequence[str]]]] = None,
) -> List[Optional[Dict[str, Any]]]:
    """Evaluate one Arrow input batch, grouping compatible rows into API calls."""
    if not (len(rows) == len(queries) == len(kinds)):
        raise JevError("Jev UDF input columns have different lengths")
    if option_columns is not None and len(option_columns) != len(rows):
        raise JevError("Jev UDF input columns have different lengths")

    results: List[Optional[Dict[str, Any]]] = [None] * len(rows)
    groups = defaultdict(list)

    for index, (row, query, raw_kind) in enumerate(zip(rows, queries, kinds)):
        # Match normal SQL NULL propagation. options may be NULL for noul.
        if row is None or query is None or raw_kind is None:
            continue

        kind = str(raw_kind).lower().strip()
        raw_options = option_columns[index] if option_columns is not None else None
        options = _normalise_options(kind, raw_options)
        row_json = _canonical_json(row)
        cache_key = (MODEL, kind, str(query), row_json, options)
        cached = _cache_get(cache_key)
        if cached is not None:
            results[index] = cached
            continue

        group_key = (kind, str(query), options)
        groups[group_key].append((index, row, cache_key))

    futures = {}
    for (kind, query, options), entries in groups.items():
        for start in range(0, len(entries), BATCH_SIZE):
            chunk = entries[start : start + BATCH_SIZE]
            future = _EXECUTOR.submit(
                _call_api,
                [entry[1] for entry in chunk],
                kind,
                query,
                options,
            )
            futures[future] = chunk

    for future in as_completed(futures):
        chunk = futures[future]
        answers = future.result()
        for (index, _row, cache_key), answer in zip(chunk, answers):
            results[index] = answer
            _cache_put(cache_key, answer)

    return results


def _constant_kinds(rows: Sequence[Any], kind: str) -> List[str]:
    return [kind] * len(rows)


@udf(
    input_types=["VARIANT", "VARCHAR"],
    result_type="BOOLEAN",
    batch_mode=True,
)
def jev(rows: List[Any], conditions: List[Optional[str]]) -> List[Optional[bool]]:
    """Return true when a row satisfies a natural-language condition."""
    answers = _evaluate(rows, conditions, _constant_kinds(rows, "noul"))
    return [
        None if answer is None else float(answer["noul"]) >= DEFAULT_THRESHOLD
        for answer in answers
    ]


@udf(
    input_types=["VARIANT", "VARCHAR"],
    result_type="DOUBLE",
    batch_mode=True,
)
def jev_prob(rows: List[Any], conditions: List[Optional[str]]) -> List[Optional[float]]:
    """Return the probability that each row satisfies a condition."""
    answers = _evaluate(rows, conditions, _constant_kinds(rows, "noul"))
    return [None if answer is None else float(answer["noul"]) for answer in answers]


@udf(
    input_types=["VARIANT", "VARCHAR", "ARRAY(VARCHAR)"],
    result_type="VARCHAR",
    batch_mode=True,
)
def jev_choice(
    rows: List[Any],
    questions: List[Optional[str]],
    options: List[Optional[List[str]]],
) -> List[Optional[str]]:
    """Classify each row into its most likely option."""
    answers = _evaluate(rows, questions, _constant_kinds(rows, "choice"), options)
    return [None if answer is None else str(answer["choice"]) for answer in answers]


@udf(
    input_types=["VARIANT", "VARCHAR", "ARRAY(VARCHAR)"],
    result_type="DOUBLE",
    batch_mode=True,
)
def jev_score(
    rows: List[Any],
    questions: List[Optional[str]],
    levels: List[Optional[List[str]]],
) -> List[Optional[float]]:
    """Return the probability-weighted position on an ordered list of levels."""
    answers = _evaluate(rows, questions, _constant_kinds(rows, "score"), levels)
    return [None if answer is None else float(answer["score"]) for answer in answers]


@udf(
    input_types=["VARIANT", "VARCHAR", "VARCHAR", "ARRAY(VARCHAR)"],
    result_type="VARIANT",
    batch_mode=True,
)
def jev_eval(
    rows: List[Any],
    questions: List[Optional[str]],
    kinds: List[str],
    options: List[Optional[List[str]]],
) -> List[Optional[Dict[str, Any]]]:
    """Return complete answers, including probabilities, legend and confidence."""
    return _evaluate(rows, questions, kinds, options)


if __name__ == "__main__":
    address = os.getenv("JEV_UDF_ADDRESS", "0.0.0.0:8815")
    metrics_address = os.getenv("JEV_METRICS_ADDRESS", "0.0.0.0:8816")
    server = UDFServer(address, metric_location=metrics_address)
    server.add_function(jev)
    server.add_function(jev_prob)
    server.add_function(jev_choice)
    server.add_function(jev_score)
    server.add_function(jev_eval)
    server.serve()
