import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import RECOMMENDED_AUTO_CUT_UDF, compile_udf


def test_recommended_use_case_auto_cut_compile_shape():
    result = compile_udf(RECOMMENDED_AUTO_CUT_UDF)

    assert result.num_args == 2
    assert result.baseline_mode is False

    assert result.ranked_cuts, "Expected at least one ranked cut for the recommended use case"
    assert result.cuts_used, "Expected at least one selected cut for the recommended use case"

    ranked_lines = {cut["line_number"] for cut in result.ranked_cuts}
    selected_line = result.cuts_used[0]["line_number"]
    assert selected_line in ranked_lines

    assert "class Operator(UDFGeneralOperator):" in result.operator_class
    assert "def process_table_0(" in result.operator_class
    assert "def process_table_1(" in result.operator_class
    assert "yield {'result': self.result}" in result.operator_class

    assert "process_table_0" in result.process_tables
    assert "process_table_1" in result.process_tables
