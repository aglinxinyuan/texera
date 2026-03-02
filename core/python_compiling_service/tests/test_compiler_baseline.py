import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.baseline import compile_baseline_mode


def test_compile_baseline_mode_generates_operator_class():
    code = """#baseline
import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    merged = pd.merge(df1, df2, on="id", how="inner")
    return merged[["id", "x"]]
"""
    result = compile_baseline_mode(code)
    assert result["baseline_mode"] is True
    assert result["ranked_cuts"] == []
    assert "class Operator(UDFGeneralOperator):" in result["operator_class"]
    assert "def process_tables(" in result["operator_class"]
