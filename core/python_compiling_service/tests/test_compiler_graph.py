import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.compiler.graph import (
    are_all_temporal_edges,
    draw_graph_output,
    find_valid_cuts,
    generate_dot_graph,
    get_topological_order,
    get_edges_crossing_line,
    has_cycle,
    visualize_graph,
    visualize_graph_text,
)


def test_get_edges_crossing_line():
    edges = {
        ("a", 2): {("a", 4), ("a", 5)},
        ("b", 3): {("b", 6)},
    }
    crossing = get_edges_crossing_line(edges, 4)
    assert (("a", 2), ("a", 4)) in crossing
    assert (("b", 3), ("b", 6)) in crossing


def test_are_all_temporal_edges():
    temporal = [(("a", 2), ("a", 4)), (("b", 3), ("b", 6))]
    mixed = [(("a", 2), ("c", 4))]
    assert are_all_temporal_edges(temporal) is True
    assert are_all_temporal_edges(mixed) is False


def test_find_valid_cuts():
    vertices = [("a", 1), ("a", 2), ("a", 4), ("b", 3), ("b", 6)]
    edges = {
        ("a", 2): {("a", 4)},
        ("b", 3): {("b", 6)},
    }
    cuts = find_valid_cuts(vertices, edges)
    cut_lines = [c["line_number"] for c in cuts]
    assert 1 not in cut_lines
    assert 4 in cut_lines


def test_has_cycle():
    vertices = [("a", 1), ("b", 2)]
    acyclic_edges = {("a", 1): {("b", 2)}}
    cyclic_edges = {("a", 1): {("b", 2)}, ("b", 2): {("a", 1)}}
    assert has_cycle(vertices, acyclic_edges) is False
    assert has_cycle(vertices, cyclic_edges) is True


def test_get_topological_order():
    vertices = [("a", 1), ("b", 2), ("c", 3)]
    edges = {("a", 1): {("b", 2)}, ("b", 2): {("c", 3)}}
    reverse_edges = {("b", 2): {("a", 1)}, ("c", 3): {("b", 2)}}
    order = get_topological_order(vertices, edges, reverse_edges)
    assert order[0] == ("a", 1)
    assert order[-1] == ("c", 3)


def test_visualize_graph():
    vertices = [("a", 1), ("b", 2)]
    edges = {("a", 1): {("b", 2)}}
    reverse_edges = {("b", 2): {("a", 1)}}
    output = visualize_graph(vertices, edges, reverse_edges)
    assert "Variable Dependency Graph:" in output
    assert "Vertex: (a, line 1)" in output


def test_visualize_graph_text():
    vertices = [("a", 1), ("b", 2)]
    edges = {("a", 1): {("b", 2)}}
    reverse_edges = {("b", 2): {("a", 1)}}
    variable_versions = {"a": ["a", "a1"], "b": ["b"]}
    output = visualize_graph_text(vertices, edges, reverse_edges, variable_versions)
    assert "Variable Dependency Graph (Visual)" in output
    assert "Root nodes" in output
    assert "Variable Versions:" in output


def test_generate_dot_graph(tmp_path):
    vertices = [("a", 1), ("a", 2)]
    edges = {("a", 1): {("a", 2)}}
    variable_lines = {"a": 2}
    out = tmp_path / "g.dot"
    path = generate_dot_graph(vertices, edges, variable_lines, str(out))
    assert path == str(out)
    content = out.read_text(encoding="utf-8")
    assert "digraph VariableDependencyGraph" in content


def test_draw_graph_output_fallback(monkeypatch, tmp_path):
    import os
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "graphviz":
            raise ImportError("forced")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    vertices = [("a", 1), ("a", 2)]
    edges = {("a", 1): {("a", 2)}}
    variable_lines = {"a": 2}
    out_base = tmp_path / "graph_out"
    path = draw_graph_output(vertices, edges, variable_lines, "png", str(out_base))
    assert path.endswith(".dot")
    assert os.path.exists(path)
