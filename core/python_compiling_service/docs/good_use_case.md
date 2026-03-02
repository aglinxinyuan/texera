# Good Use Case: Auto-Cut UDF Compile

This is the recommended path for production usage:

1. Submit a pandas-style UDF with 2+ input tables.
2. Do not add a `#<line>` directive.
3. Let the compiler pick the best split point automatically.

## Why this is the default

- It keeps behavior stable across UDF edits.
- It preserves compile flexibility (ranking + fallback cuts).
- It avoids hard-coding line numbers that drift over time.

## Example code

The canonical sample is defined in:

- `src/compiler/use_cases.py` as `RECOMMENDED_AUTO_CUT_UDF`

## Run the local example

```bash
cd core/python_compiling_service
.venv/bin/python examples/good_use_case.py
```

Expected summary shape:

- `num_args: 2`
- `baseline_mode: False`
- `ranked_cuts: [...]` (non-empty)
- `cuts_used: [...]` (first selected cut)
- generated operator class containing `process_table_0` and `process_table_1`

## HTTP service usage

```bash
curl -s -X POST http://localhost:9999/compile \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "code": "import pandas as pd\n\ndef score_events(df_events: pd.DataFrame, df_weights: pd.DataFrame) -> pd.DataFrame:\n    active = df_events[df_events[\"event\"] != \"idle\"]\n    joined = pd.merge(active, df_weights, on=\"event\", how=\"left\")\n    joined[\"score\"] = joined[\"count\"] * joined[\"weight\"].fillna(0)\n    result = joined[[\"user_id\", \"event\", \"score\"]]\n    return result\n"
}
JSON
```

The response is plain text Python code for the generated operator class.
