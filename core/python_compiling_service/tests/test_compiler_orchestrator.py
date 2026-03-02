import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.orchestrator import compile_non_baseline


def test_compile_non_baseline_orchestrates_steps():
    lines = ["import pandas as pd", "def f(df1, df2):", "    return df1"]

    def preprocess(code):
        assert "def f" in code
        return "cleaned"

    def infer_types(cleaned):
        assert cleaned == "cleaned"
        return {"df1": "DataFrame", "df2": "DataFrame"}

    def ssa(cleaned):
        assert cleaned == "cleaned"
        return "ssa_code"

    def convert(ssa_code, cleaned_code, type_info):
        assert ssa_code == "ssa_code"
        assert cleaned_code == "cleaned"
        assert "df1" in type_info
        return "converted_code"

    class MockGraph:
        def __init__(self, ssa_code):
            assert ssa_code == "ssa_code"
            self.vertices = [("df1_1", 1)]
            self.variable_types = {}

        def _get_base_variable_name(self, var_name):
            return var_name.split("_")[0]

        def find_valid_cuts(self):
            return [{"line_number": 3, "crossing_edges": [], "description": "ok"}]

        def rank_cuts_by_variable_size(self, valid_cuts):
            return valid_cuts

    def split(converted, ranked_cuts, cleaned, type_info):
        assert converted == "converted_code"
        assert ranked_cuts[0]["line_number"] == 3
        assert cleaned == "cleaned"
        assert "df1" in type_info
        return {
            "process_tables": {"process_table_0": "x"},
            "operator_class": "class Operator: pass\n",
            "num_args": 2,
            "cuts_used": [],
            "filtered_cuts": ranked_cuts,
        }

    def infer_port_assignments(cleaned):
        assert cleaned == "cleaned"
        return [{"line_number": 2, "statement": "x = 1", "port": "df1_port"}]

    result = compile_non_baseline(
        lines=lines,
        line_number=None,
        preprocess_code_fn=preprocess,
        infer_types_fn=infer_types,
        ssa_fn=ssa,
        convert_ssa_fn=convert,
        graph_cls=MockGraph,
        split_fn=split,
        infer_port_assignments_fn=infer_port_assignments,
    )

    assert result["ssa_code"] == "ssa_code"
    assert result["converted_code"] == "converted_code"
    assert result["ranked_cuts"][0]["line_number"] == 3
    assert result["operator_class"].startswith("import pandas as pd\n\n")
    assert result["port_assignments"] == [{"line_number": 2, "statement": "x = 1", "port": "df1_port"}]
