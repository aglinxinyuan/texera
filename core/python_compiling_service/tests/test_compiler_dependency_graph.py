import ast
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.dependency_graph import DependencyVisitor, VariableDependencyGraph


def test_dependency_visitor_extracts_types_from_annotations_and_assignments():
    code = """
import pandas as pd

def f(df: pd.DataFrame):
    s = df["col"]
    return s
"""
    tree = ast.parse(code)
    visitor = DependencyVisitor()
    visitor.visit(tree)

    assert visitor.variable_types["df"] == "DataFrame"
    assert visitor.variable_types["s"] == "Series"


def test_variable_dependency_graph_builds_vertices_and_is_acyclic():
    ssa_code = """
def f(x):
    y = x
    z = y
    return z
"""
    graph = VariableDependencyGraph(ssa_code)

    y_vertices = graph.get_vertices_by_variable("y")
    z_vertices = graph.get_vertices_by_variable("z")
    assert y_vertices
    assert z_vertices
    assert graph.has_cycle() is False
    assert isinstance(graph.find_valid_cuts(), list)
