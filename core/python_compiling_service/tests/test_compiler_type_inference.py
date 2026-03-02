import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.type_inference import infer_types_from_code


def test_infer_types_from_annotations_and_assignments():
    code = """
import pandas as pd

def f(df: pd.DataFrame, n: int):
    s = df["col"]
    x = 1
    z = x + n
    a, b = ("txt", 3)
    return z
"""
    type_info = infer_types_from_code(code)

    assert type_info["df"] == "DataFrame"
    assert type_info["n"] == "int"
    assert type_info["s"] == "Series"
    assert type_info["x"] == "numeric"
    assert type_info["z"] == "int"
    assert type_info["a"] == "str"
    assert type_info["b"] == "numeric"


def test_infer_types_returns_empty_on_invalid_code():
    assert infer_types_from_code("def bad(:\n    pass") == {}
