import os
import sys

import pytest

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import compile_udf


CASE_CODES = {
    "filter_merge_project": """import pandas as pd

def filter_merge_project(df_events: pd.DataFrame, df_users: pd.DataFrame) -> pd.DataFrame:
    valid = df_events[df_events["event"] != "idle"]
    joined = pd.merge(valid, df_users, on="user_id", how="left")
    enriched = joined[["user_id", "event", "country"]]
    return enriched
""",
    "groupby_then_join": """import pandas as pd

def groupby_then_join(df_sales: pd.DataFrame, df_region: pd.DataFrame) -> pd.DataFrame:
    summary = df_sales.groupby("region_id", as_index=False)["amount"].sum()
    summary = summary.rename(columns={"amount": "total_amount"})
    joined = pd.merge(summary, df_region, on="region_id", how="inner")
    result = joined[["region_id", "region_name", "total_amount"]]
    return result
""",
    "string_clean_match": """import pandas as pd

def string_clean_match(df_left: pd.DataFrame, df_right: pd.DataFrame) -> pd.DataFrame:
    left_clean = df_left["text"].fillna("").str.strip().str.lower()
    right_clean = df_right["text"].fillna("").str.strip().str.lower()
    merged = pd.DataFrame({"l": left_clean, "r": right_clean})
    merged["is_match"] = merged["l"] == merged["r"]
    return merged
""",
    "datetime_join_score": """import pandas as pd

def datetime_join_score(df_logs: pd.DataFrame, df_weights: pd.DataFrame) -> pd.DataFrame:
    logs = df_logs.copy()
    logs["event_day"] = pd.to_datetime(logs["ts"]).dt.date
    joined = pd.merge(logs, df_weights, on="event_type", how="left")
    joined["score"] = joined["cnt"] * joined["weight"].fillna(1.0)
    output = joined[["user_id", "event_day", "score"]]
    return output
""",
    "mask_and_calculate": """import pandas as pd

def mask_and_calculate(df_orders: pd.DataFrame, df_tax: pd.DataFrame) -> pd.DataFrame:
    paid = df_orders[df_orders["status"] == "paid"]
    joined = pd.merge(paid, df_tax, on="state", how="left")
    joined["final_price"] = joined["price"] * (1 + joined["tax_rate"].fillna(0))
    result = joined[["order_id", "state", "final_price"]]
    return result
""",
}


@pytest.mark.parametrize("case_name,code", CASE_CODES.items())
def test_auto_cut_for_two_input_cases(case_name: str, code: str):
    result = compile_udf(code)

    assert result.num_args == 2, f"{case_name}: expected 2 input args"
    assert result.baseline_mode is False, f"{case_name}: should be auto mode"

    assert result.ranked_cuts, f"{case_name}: expected ranked cuts"
    assert result.cuts_used, f"{case_name}: expected selected cuts"

    ranked_lines = {cut["line_number"] for cut in result.ranked_cuts}
    used_lines = [cut["line_number"] for cut in result.cuts_used]
    for line in used_lines:
        assert line in ranked_lines, f"{case_name}: used line {line} not in ranked cuts"

    assert "process_table_0" in result.process_tables, f"{case_name}: missing process_table_0"
    assert "process_table_1" in result.process_tables, f"{case_name}: missing process_table_1"
    assert "def process_table_0(" in result.operator_class, f"{case_name}: missing method 0 in operator"
    assert "def process_table_1(" in result.operator_class, f"{case_name}: missing method 1 in operator"
