# Python Compiling Service

This service compiles user-provided Python UDF code (typically pandas-style functions)
into executable Texera operator code (`class Operator(UDFGeneralOperator)`).

It supports:

- Auto cut selection (recommended): compiler chooses where to split the UDF into stages.
- Explicit cut selection (for experiment): caller forces split line with `#<line>`.
- Baseline mode (for experiment): no staged split, single process method via `#baseline`.

## What Is Input / Output

Input:

- A Python code string containing one function definition.
- Optional first-line directive:
  - `#baseline`
  - `#<line_number>`
  - or no directive (auto mode).

Output:

- HTTP `/compile` returns plain text operator class code.
- Python facade `compile_udf(...)` returns a typed result object with:
  - `operator_class`
  - `ranked_cuts`
  - `cuts_used`
  - `num_args`
  - `process_tables`
  - `port_assignments` (experimental statement-to-port labeling metadata)
  - and debug fields (`ssa_code`, `converted_code`, etc.).

## Entrypoints

- HTTP service: `src/udf_compiling_service.py`
- Compiler facade: `src/compiler/facade.py`
  - `compile_udf(code, line_number=None)`
  - `compile_udf_legacy(code, line_number=None)`

## Run Locally

```bash
cd core/python_compiling_service
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python src/udf_compiling_service.py
```

Service default URL: `http://localhost:9999`

## HTTP API

### `GET /health`

Health check endpoint.

### `GET /example`

Returns canonical sample payloads for:

- explicit line cut
- baseline mode
- auto mode

### `POST /compile`

Request body:

```json
{
  "code": "import pandas as pd\n\ndef f(df1: pd.DataFrame, df2: pd.DataFrame):\n    ...\n"
}
```

Response:

- `200` with plain text compiled operator code.
- `400` for invalid payload.
- `500` for internal compile errors.

Example:

```bash
curl -s -X POST http://localhost:9999/compile \
  -H 'Content-Type: application/json' \
  -d '{"code":"import pandas as pd\n\ndef f(df1: pd.DataFrame, df2: pd.DataFrame):\n    x = pd.merge(df1, df2, on=\"id\")\n    return x\n"}'
```

## Python Usage

```python
from compiler import compile_udf

code = """
import pandas as pd

def f(df1: pd.DataFrame, df2: pd.DataFrame):
    x = pd.merge(df1, df2, on="id")
    return x
"""

result = compile_udf(code)
print(result.num_args)
print([c["line_number"] for c in result.cuts_used])
print(result.operator_class)
```

## Recommended Use Case

Use two-input UDF + auto mode (no prefix). See:

- `docs/good_use_case.md`
- `examples/good_use_case.py`
- `src/compiler/use_cases.py` (`RECOMMENDED_AUTO_CUT_UDF`)

## Architecture / Source Layout

- `docs/architecture.md`: runtime call flow and module layering.
- `src/compiler/`: compiler core (SSA, dependency graph, cut ranking, splitter).
- `tests/`: regression tests for compile behavior, API, and auto-cut cases.

## Test

```bash
cd core/python_compiling_service
.venv/bin/pytest -q
```
