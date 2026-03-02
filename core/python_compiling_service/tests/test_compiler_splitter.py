import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.splitter import (
    apply_loop_transformation_to_process_table,
    generate_process_tables_and_split,
)


def test_apply_loop_transformation_returns_original_for_invalid_method():
    code = "not_a_method_definition"
    assert apply_loop_transformation_to_process_table(code, "process_table_0") == code


def test_generate_process_tables_single_arg_converts_return_to_yield(monkeypatch):
    monkeypatch.setattr(
        "src.compiler.splitter.apply_loop_transformation_to_process_table",
        lambda process_table_code, table_name: process_table_code,
    )

    converted_code = """
def f(x):
    self.tmp = x
    return self.tmp
"""
    result = generate_process_tables_and_split(
        converted_code=converted_code,
        ranked_cuts=[],
        original_code=converted_code,
        type_info={"x": "int", "tmp": "int"},
    )

    process_table_0 = result["process_tables"]["process_table_0"]
    assert "def process_table_0(self, x: int):" in process_table_0
    assert "self.tmp: int = x" in process_table_0
    assert "yield self.tmp" in process_table_0
    assert result["num_args"] == 1
    assert result["cuts_used"] == []


def test_generate_process_tables_multi_arg_uses_cut_and_adds_yield_none(monkeypatch):
    monkeypatch.setattr(
        "src.compiler.splitter.apply_loop_transformation_to_process_table",
        lambda process_table_code, table_name: process_table_code,
    )

    converted_code = """
def f(df1, df2):
    self.left = df1
    self.right = df2
    self.joined = self.left
    return self.joined
"""
    ranked_cuts = [{"line_number": 4, "description": "cut", "crossing_edges": []}]

    result = generate_process_tables_and_split(
        converted_code=converted_code,
        ranked_cuts=ranked_cuts,
        original_code=converted_code,
        type_info={"df1": "DataFrame", "df2": "DataFrame", "left": "Series", "right": "Series"},
    )

    table_0 = result["process_tables"]["process_table_0"]
    table_1 = result["process_tables"]["process_table_1"]

    assert "def process_table_0(self, df1: pd.DataFrame, df2: pd.DataFrame):" in table_0
    assert "yield None" in table_0
    assert "def process_table_1(self):" in table_1
    assert "yield self.joined" in table_1
    assert result["num_args"] == 2
    assert result["cuts_used"][0]["line_number"] == 4
    assert result["filtered_cuts"][0]["line_number"] == 4
