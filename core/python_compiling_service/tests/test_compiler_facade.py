import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import compile_udf, compile_udf_legacy


def test_compile_udf_facade_matches_legacy_compile():
    code = """import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    filtered = df1[df1["x"] > 0]
    merged = pd.merge(filtered, df2, on="id", how="inner")
    return merged[["id", "x"]]
"""
    legacy = compile_udf_legacy(code)
    facade = compile_udf(code)

    assert facade.operator_class == legacy["operator_class"]
    assert facade.num_args == legacy["num_args"]
    assert [c["line_number"] for c in facade.cuts_used] == [
        c["line_number"] for c in legacy["cuts_used"]
    ]
    assert isinstance(facade.port_assignments, list)
