import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.common import (
    get_base_variable_name,
    infer_line_number_from_code,
)


def test_get_base_variable_name():
    assert get_base_variable_name("X1") == "X1"
    assert get_base_variable_name("table_12") == "table_12"
    assert get_base_variable_name("value99") == "value99"
    assert get_base_variable_name("plain") == "plain"


def test_infer_line_number_from_code():
    assert infer_line_number_from_code("#5\nx=1") == 5
    assert infer_line_number_from_code("#baseline\nx=1") is None
    assert infer_line_number_from_code("x=1") is None
