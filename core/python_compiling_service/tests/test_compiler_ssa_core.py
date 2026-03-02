import ast
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.ssa_core import SSA


def test_ssa_rewrites_assign_and_augassign():
    code = """
def f(x):
    y = x + 1
    y += 2
    return y
"""
    ssa_code = SSA(code)
    tree = ast.parse(ssa_code)
    func = tree.body[0]

    assign_0 = func.body[0]
    assign_1 = func.body[1]
    ret = func.body[2]

    assert isinstance(assign_0, ast.Assign)
    assert isinstance(assign_0.targets[0], ast.Name)
    assert assign_0.targets[0].id == "y"

    assert isinstance(assign_1, ast.Assign)
    assert isinstance(assign_1.targets[0], ast.Name)
    assert assign_1.targets[0].id == "y1"

    assert isinstance(ret, ast.Return)
    assert isinstance(ret.value, ast.Name)
    assert ret.value.id == "y1"


def test_ssa_handles_tuple_assignment_with_temporaries():
    code = """
def f(a, b):
    a, b = b, a
    return a
"""
    ssa_code = SSA(code)

    assert "_tmp_0" in ssa_code
    assert "_tmp_1" in ssa_code
    assert "a1 = _tmp_0" in ssa_code
    assert "b1 = _tmp_1" in ssa_code
    assert "return a1" in ssa_code


def test_ssa_raises_value_error_on_invalid_code():
    try:
        SSA("def bad(:\n    pass")
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "Failed to convert code to SSA format" in str(e)
