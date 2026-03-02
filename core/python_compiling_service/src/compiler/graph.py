import logging
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Sequence, Set, Tuple

Vertex = Tuple[str, int]
Edge = Tuple[Vertex, Vertex]
logger = logging.getLogger(__name__)


def get_edges_crossing_line(edges: Dict[Vertex, Set[Vertex]], line_num: int) -> List[Edge]:
    crossing_edges: List[Edge] = []
    for from_vertex, to_vertices in edges.items():
        _from_var, from_line = from_vertex
        for to_vertex in to_vertices:
            _to_var, to_line = to_vertex
            if from_line < line_num and to_line >= line_num:
                crossing_edges.append((from_vertex, to_vertex))
    return crossing_edges


def are_all_temporal_edges(edges: Iterable[Edge]) -> bool:
    for from_vertex, to_vertex in edges:
        from_var, _from_line = from_vertex
        to_var, _to_line = to_vertex
        if from_var != to_var:
            return False
    return True


def find_valid_cuts(vertices: Sequence[Vertex], edges: Dict[Vertex, Set[Vertex]]) -> List[dict]:
    valid_cuts: List[dict] = []

    line_groups = defaultdict(list)
    for var, line in vertices:
        line_groups[line].append((var, line))

    for line_num in sorted(line_groups.keys()):
        if line_num == 1:
            continue

        crossing_edges = get_edges_crossing_line(edges, line_num)
        if are_all_temporal_edges(crossing_edges):
            valid_cuts.append(
                {
                    "line_number": line_num,
                    "crossing_edges": crossing_edges,
                    "description": f"Valid cut at line {line_num} - cuts {len(crossing_edges)} temporal edge(s)",
                }
            )

    return valid_cuts


def has_cycle(vertices: Sequence[Vertex], edges: Dict[Vertex, Set[Vertex]]) -> bool:
    visited = set()
    rec_stack = set()

    def dfs(vertex: Vertex) -> bool:
        visited.add(vertex)
        rec_stack.add(vertex)

        for neighbor in edges.get(vertex, set()):
            if neighbor not in visited:
                if dfs(neighbor):
                    return True
            elif neighbor in rec_stack:
                return True

        rec_stack.remove(vertex)
        return False

    for vertex in vertices:
        if vertex not in visited:
            if dfs(vertex):
                return True
    return False


def get_topological_order(
    vertices: Sequence[Vertex], edges: Dict[Vertex, Set[Vertex]], reverse_edges: Dict[Vertex, Set[Vertex]]
) -> List[Vertex]:
    if has_cycle(vertices, edges):
        raise ValueError("Cannot get topological order: graph contains cycles")

    in_degree = defaultdict(int)
    for vertex in vertices:
        in_degree[vertex] = len(reverse_edges.get(vertex, set()))

    queue = deque([vertex for vertex in vertices if in_degree[vertex] == 0])
    result: List[Vertex] = []

    while queue:
        vertex = queue.popleft()
        result.append(vertex)

        for dependent in edges.get(vertex, set()):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    return result


def visualize_graph(vertices: Sequence[Vertex], edges: Dict[Vertex, Set[Vertex]], reverse_edges: Dict[Vertex, Set[Vertex]]) -> str:
    result = "Variable Dependency Graph:\n"
    result += "=" * 30 + "\n"

    for vertex in sorted(vertices):
        variable, line_num = vertex
        dependents = list(edges.get(vertex, set()))
        dependencies = list(reverse_edges.get(vertex, set()))

        result += f"Vertex: ({variable}, line {line_num})\n"
        result += f"  Dependencies: {dependencies}\n"
        result += f"  Dependents: {dependents}\n"
        result += "-" * 20 + "\n"

    return result


def _find_paths_to_leaves(start: Vertex, leaf_nodes: Sequence[Vertex], edges: Dict[Vertex, Set[Vertex]]) -> List[List[Vertex]]:
    paths: List[List[Vertex]] = []

    def dfs(current: Vertex, path: List[Vertex], visited: Set[Vertex]) -> None:
        path.append(current)
        visited.add(current)

        if current in leaf_nodes:
            paths.append(path[:])
        else:
            for neighbor in edges.get(current, set()):
                if neighbor not in visited:
                    dfs(neighbor, path, visited)

        path.pop()
        visited.remove(current)

    dfs(start, [], set())
    return paths


def visualize_graph_text(
    vertices: Sequence[Vertex],
    edges: Dict[Vertex, Set[Vertex]],
    reverse_edges: Dict[Vertex, Set[Vertex]],
    variable_versions: Dict[str, List[str]],
) -> str:
    if not vertices:
        return "Empty graph"

    sorted_vertices = sorted(vertices, key=lambda x: (x[0], x[1]))

    result: List[str] = []
    result.append("Variable Dependency Graph (Visual)")
    result.append("=" * 50)
    result.append("")

    matrix = []
    for vertex in sorted_vertices:
        row = []
        for other_vertex in sorted_vertices:
            row.append("1" if other_vertex in edges.get(vertex, set()) else "0")
        matrix.append(row)

    header = "    " + " ".join(f"({v},{l})" for v, l in sorted_vertices)
    result.append(header)
    result.append("    " + "-" * (len(sorted_vertices) * 12))

    for i, vertex in enumerate(sorted_vertices):
        var, line = vertex
        row_str = f"({var},{line}) |"
        for cell in matrix[i]:
            row_str += f" {cell:>10}"
        result.append(row_str)

    result.append("")
    result.append("Legend: 1 = dependency exists, 0 = no dependency")
    result.append("        Row → Column: Row depends on Column")
    result.append("")

    result.append("Vertices (Variable, Line Number):")
    result.append("-" * 35)
    for vertex in sorted_vertices:
        var, line = vertex
        dependents = list(edges.get(vertex, set()))
        dependencies = list(reverse_edges.get(vertex, set()))
        result.append(f"({var}, {line}): deps={dependencies}, dependents={dependents}")

    result.append("")
    result.append("Dependency Chains:")
    result.append("-" * 20)

    root_nodes = [v for v in sorted_vertices if not reverse_edges.get(v, set())]
    if root_nodes:
        result.append(f"Root nodes (no dependencies): {root_nodes}")
    else:
        result.append("No root nodes found (possible cycles)")

    leaf_nodes = [v for v in sorted_vertices if not edges.get(v, set())]
    if leaf_nodes:
        result.append(f"Leaf nodes (no dependents): {leaf_nodes}")

    result.append("")
    result.append("Example Dependency Paths:")
    result.append("-" * 25)

    for root in root_nodes[:3]:
        paths = _find_paths_to_leaves(root, leaf_nodes, edges)
        for path in paths[:2]:
            path_str = " → ".join(f"({v},{l})" for v, l in path)
            result.append(f"({root[0]},{root[1]}) → ... → ({path[-1][0]},{path[-1][1]}): {path_str}")

    result.append("")
    result.append("Variable Versions:")
    result.append("-" * 18)
    for base_var in sorted(set(v for v, _l in sorted_vertices)):
        versions = variable_versions.get(base_var, [])
        if len(versions) > 1:
            result.append(f"{base_var}: {versions}")

    return "\n".join(result)


def generate_dot_graph(
    vertices: Sequence[Vertex],
    edges: Dict[Vertex, Set[Vertex]],
    variable_lines: Dict[str, int],
    filename: str = "dependency_graph.dot",
) -> str:
    dot_content: List[str] = []
    dot_content.append("digraph VariableDependencyGraph {")
    dot_content.append("    rankdir=TB;")
    dot_content.append('    node [shape=box, style=filled, fontname="Arial"];')
    dot_content.append('    edge [fontname="Arial", fontsize=10];')
    dot_content.append("")

    line_groups = defaultdict(list)
    for vertex in sorted(vertices, key=lambda x: (x[0], x[1])):
        var, line = vertex
        line_groups[line].append(vertex)

    for line_num in sorted(line_groups.keys()):
        dot_content.append(f"    subgraph cluster_line_{line_num} {{")
        dot_content.append("        rank=same;")
        dot_content.append(f'        label="Line {line_num}";')
        dot_content.append("        style=invis;")
        dot_content.append("")

        for vertex in line_groups[line_num]:
            var, line = vertex
            if var in ["X", "Y"]:
                color = "lightblue"
            elif var.endswith("1"):
                color = "lightgreen"
            else:
                color = "lightyellow"

            dot_content.append(f'        "{var}_{line}" [label="{var}\\n(line {line})", fillcolor="{color}"];')

        dot_content.append("    }")
        dot_content.append("")

    for from_vertex, to_vertices in edges.items():
        from_var, from_line = from_vertex
        for to_vertex in to_vertices:
            to_var, to_line = to_vertex
            if from_var == to_var:
                edge_style = "[color=blue, style=dashed]"
                dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')
            else:
                is_assignment = to_line in variable_lines and variable_lines.get(to_var) == to_line
                edge_style = "[color=red, style=solid]"
                if is_assignment:
                    dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')
                else:
                    dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')

    dot_content.append("}")

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(dot_content))

    return filename


def draw_graph_output(
    vertices: Sequence[Vertex],
    edges: Dict[Vertex, Set[Vertex]],
    variable_lines: Dict[str, int],
    output_format: str = "png",
    filename: str = "dependency_graph",
) -> str:
    try:
        import graphviz

        dot = graphviz.Digraph(comment="Variable Dependency Graph")
        dot.attr(rankdir="TB")
        dot.attr("node", shape="box", style="filled", fontname="Arial")
        dot.attr("edge", fontname="Arial", fontsize="10")

        line_groups = defaultdict(list)
        for vertex in sorted(vertices, key=lambda x: (x[0], x[1])):
            var, line = vertex
            line_groups[line].append(vertex)

        for line_num in sorted(line_groups.keys()):
            with dot.subgraph(name=f"cluster_line_{line_num}") as subgraph:
                subgraph.attr(rank="same")
                subgraph.attr(label=f"Line {line_num}")
                subgraph.attr(style="invis")

                for vertex in line_groups[line_num]:
                    var, line = vertex
                    node_id = f"{var}_{line}"
                    if var in ["X", "Y"]:
                        color = "lightblue"
                    elif var.endswith("1"):
                        color = "lightgreen"
                    else:
                        color = "lightyellow"
                    subgraph.node(node_id, f"{var}\n(line {line})", fillcolor=color)

        for from_vertex, to_vertices in edges.items():
            from_var, from_line = from_vertex
            for to_vertex in to_vertices:
                to_var, to_line = to_vertex
                if from_var == to_var:
                    dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", color="blue", style="dashed")
                else:
                    is_assignment = to_line in variable_lines and variable_lines.get(to_var) == to_line
                    if is_assignment:
                        dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", color="red", style="solid")
                    else:
                        dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", color="red", style="solid")

        output_file = f"{filename}.{output_format}"
        dot.render(filename, format=output_format, cleanup=True)
        logger.info("Graph saved as: %s", output_file)
        return output_file

    except ImportError:
        logger.warning("Graphviz not available. Generating DOT file instead.")
        return generate_dot_graph(vertices, edges, variable_lines, f"{filename}.dot")
