import os
import sys
import unittest

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler import compile_udf_legacy as compile


AUTO_CODE = """import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    filtered = df1[df1["x"] > 0]
    merged = pd.merge(filtered, df2, on="id", how="inner")
    projected = merged[["id", "x"]]
    return projected
"""


BASELINE_CODE = """#baseline
import pandas as pd

def enrich(df1: pd.DataFrame, df2: pd.DataFrame):
    merged = pd.merge(df1, df2, on="id", how="inner")
    return merged[["id", "x"]]
"""


class CompileBehaviorTest(unittest.TestCase):
    def test_compile_baseline_mode(self):
        result = compile(BASELINE_CODE)

        self.assertTrue(result["baseline_mode"])
        self.assertEqual(result["ranked_cuts"], [])
        self.assertIn("class Operator(UDFGeneralOperator):", result["operator_class"])
        self.assertIn("def process_tables(", result["operator_class"])
        self.assertIn("yield", result["operator_class"])

    def test_compile_auto_contains_expected_structure(self):
        result = compile(AUTO_CODE)

        self.assertIn("ranked_cuts", result)
        self.assertIn("operator_class", result)
        self.assertIn("process_tables", result)
        self.assertEqual(result["num_args"], 2)
        self.assertIn("class Operator(UDFGeneralOperator):", result["operator_class"])
        self.assertIn("def process_table_", result["operator_class"])
        self.assertEqual(result["import_statements"], ["import pandas as pd"])

    def test_compile_with_explicit_valid_line_number_uses_single_cut(self):
        auto_result = compile(AUTO_CODE)
        self.assertTrue(auto_result["ranked_cuts"], "Expected at least one valid cut for this input")

        selected_line = auto_result["ranked_cuts"][0]["line_number"]
        explicit_result = compile(AUTO_CODE, line_number=selected_line)

        self.assertEqual(len(explicit_result["ranked_cuts"]), 1)
        self.assertEqual(explicit_result["ranked_cuts"][0]["line_number"], selected_line)

    def test_compile_with_invalid_line_number_falls_back_to_auto(self):
        auto_result = compile(AUTO_CODE)
        invalid_line_result = compile(AUTO_CODE, line_number=99999)

        self.assertEqual(invalid_line_result["ranked_cuts"], auto_result["ranked_cuts"])


class CompileErrorHandlingTest(unittest.TestCase):
    def test_compile_baseline_rejects_non_function_input(self):
        bad_code = """#baseline
import pandas as pd

x = 1
"""
        with self.assertRaisesRegex(ValueError, "Failed to compile in baseline mode"):
            compile(bad_code)
