import ast
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.preprocessing import preprocess_code


def _has_docstring_stmt(node):
    if not getattr(node, "body", None):
        return False
    first = node.body[0]
    return isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant) and isinstance(first.value.value, str)


def test_preprocess_removes_docstrings_comments_and_blank_lines():
    code = '''"""module doc"""
# top comment
def f(x):
    """function doc"""
    y = x + 1  # inline comment
    return y
'''
    cleaned = preprocess_code(code)
    tree = ast.parse(cleaned)

    assert not _has_docstring_stmt(tree)
    func = tree.body[0]
    assert isinstance(func, ast.FunctionDef)
    assert not _has_docstring_stmt(func)
    assert "inline comment" not in cleaned


def test_preprocess_falls_back_when_cleaned_code_becomes_unparseable():
    code = '''def f():
    """only docstring"""
'''
    cleaned = preprocess_code(code)
    assert cleaned.strip() == "def f():"


def test_preprocess_keeps_valid_module_when_input_has_only_comments():
    cleaned = preprocess_code("# only comments")
    tree = ast.parse(cleaned)
    assert len(tree.body) == 1
    assert isinstance(tree.body[0], ast.Pass)
