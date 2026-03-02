import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.pipeline import (
    apply_type_info_to_graph,
    assemble_compile_result,
    build_ranked_cuts,
    maybe_select_ranked_cut,
    prepend_imports_to_operator_class,
    split_imports_and_function_code,
)


def test_split_imports_and_function_code():
    lines = [
        "import pandas as pd",
        "from math import sqrt",
        "",
        "def f(x):",
        "    return x",
    ]
    imports, function_code = split_imports_and_function_code(lines)
    assert imports == ["import pandas as pd", "from math import sqrt"]
    assert "def f(x):" in function_code


def test_maybe_select_ranked_cut():
    cuts = [{"line_number": 3}, {"line_number": 5}]
    assert maybe_select_ranked_cut(cuts, None) == cuts
    assert maybe_select_ranked_cut(cuts, 5) == [{"line_number": 5}]
    assert maybe_select_ranked_cut(cuts, 100) == cuts


def test_prepend_imports_to_operator_class():
    operator_class = "from pytexera import *\nclass Operator: pass\n"
    with_imports = prepend_imports_to_operator_class(
        ["  import pandas as pd", "from math import sqrt"], operator_class
    )
    assert with_imports.startswith("import pandas as pd\nfrom math import sqrt\n\n")
    assert "class Operator: pass" in with_imports


def test_apply_type_info_to_graph():
    class MockGraph:
        def __init__(self):
            self.vertices = [("a_1", 1), ("b_1", 2)]
            self.variable_types = {}

        def _get_base_variable_name(self, var_name):
            return var_name.split("_")[0]

    graph = MockGraph()
    apply_type_info_to_graph(graph, {"a": "DataFrame"})
    assert graph.variable_types["a_1"] == "DataFrame"
    assert "b_1" not in graph.variable_types


def test_build_ranked_cuts():
    class MockGraph:
        def find_valid_cuts(self):
            return [{"line_number": 3}, {"line_number": 5}]

        def rank_cuts_by_variable_size(self, valid_cuts):
            return valid_cuts

    graph = MockGraph()
    assert build_ranked_cuts(graph, None) == [{"line_number": 3}, {"line_number": 5}]
    assert build_ranked_cuts(graph, 5) == [{"line_number": 5}]


def test_assemble_compile_result():
    result = assemble_compile_result(
        ranked_cuts=[{"line_number": 3}],
        ssa_code="ssa",
        converted_code="converted",
        split_result={
            "process_tables": {"process_table_0": "x"},
            "operator_class": "class Operator: pass\n",
            "num_args": 1,
            "cuts_used": [],
            "filtered_cuts": [],
        },
        import_statements=["import pandas as pd"],
        cleaned_code="cleaned",
    )
    assert result["ranked_cuts"] == [{"line_number": 3}]
    assert result["num_args"] == 1
    assert result["operator_class"].startswith("import pandas as pd\n\n")
