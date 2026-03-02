import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import SSA
from src.compiler.ssa_transform import convert_ssa_to_self


def test_convert_ssa_to_self_transforms_local_variables():
    cleaned_function_code = """def f(df1, df2):
    merged = df1
    projected = merged
    return projected
"""
    ssa_code = SSA(cleaned_function_code)
    converted = convert_ssa_to_self(ssa_code, cleaned_function_code, {"merged": "DataFrame"})

    assert "self.merged" in converted
    assert "self.projected" in converted
