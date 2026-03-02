import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.cut_strategy import estimate_variable_size, rank_cuts_by_variable_size


def test_estimate_variable_size():
    sizes = {"DataFrame": 100000, "unknown": 8}
    assert estimate_variable_size({"a": "DataFrame"}, "a", sizes) == 100000
    assert estimate_variable_size({}, "missing", sizes) == 8


def test_rank_cuts_by_variable_size_orders_by_score():
    cuts = [
        {
            "line_number": 4,
            "crossing_edges": [(("small", 2), ("small", 4))],
            "description": "small cut",
        },
        {
            "line_number": 5,
            "crossing_edges": [(("big", 2), ("big", 5))],
            "description": "big cut",
        },
    ]
    vertices = [("arg1", 5), ("small", 2), ("big", 2)]
    variable_types = {"small": "unknown", "big": "DataFrame", "arg1": "DataFrame"}
    type_size = {"unknown": 8, "DataFrame": 100000}

    ranked = rank_cuts_by_variable_size(
        valid_cuts=cuts,
        vertices=vertices,
        variable_types=variable_types,
        type_size_bytes=type_size,
        first_usage_heuristic_bonus=-50000,
    )

    assert ranked[0]["line_number"] == 4
    assert ranked[1]["line_number"] == 5
