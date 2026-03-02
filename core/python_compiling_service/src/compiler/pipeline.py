import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def split_imports_and_function_code(lines: List[str]) -> Tuple[List[str], str]:
    import_statements: List[str] = []
    function_code: List[str] = []

    for line in lines:
        stripped_line = line.strip()
        if stripped_line.startswith("import ") or stripped_line.startswith("from "):
            import_statements.append(line)
        else:
            function_code.append(line)

    return import_statements, "\n".join(function_code)


def maybe_select_ranked_cut(ranked_cuts: List[dict], line_number: Optional[int]) -> List[dict]:
    if line_number is None:
        return ranked_cuts

    specified_cut = None
    for cut in ranked_cuts:
        if cut["line_number"] == line_number:
            specified_cut = cut
            break

    if specified_cut:
        return [specified_cut]

    logger.warning("Line %s is not a valid cut point. Using best available cut.", line_number)
    return ranked_cuts


def prepend_imports_to_operator_class(import_statements: List[str], operator_class: str) -> str:
    if not import_statements:
        return operator_class

    clean_imports = [imp.lstrip() for imp in import_statements]
    return "\n".join(clean_imports) + "\n\n" + operator_class


def apply_type_info_to_graph(graph: Any, type_info: Dict[str, str]) -> None:
    if not type_info:
        return

    for var_name, var_type in type_info.items():
        for vertex in graph.vertices:
            vertex_var, _ = vertex
            if graph._get_base_variable_name(vertex_var) == var_name:
                graph.variable_types[vertex_var] = var_type


def build_ranked_cuts(graph: Any, line_number: Optional[int]) -> List[dict]:
    valid_cuts = graph.find_valid_cuts()
    ranked_cuts = graph.rank_cuts_by_variable_size(valid_cuts)
    return maybe_select_ranked_cut(ranked_cuts, line_number)


def assemble_compile_result(
    *,
    ranked_cuts: List[dict],
    ssa_code: str,
    converted_code: str,
    split_result: Dict[str, Any],
    import_statements: List[str],
    cleaned_code: str,
    port_assignments: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    operator_class_with_imports = prepend_imports_to_operator_class(
        import_statements, split_result["operator_class"]
    )

    return {
        "ranked_cuts": ranked_cuts,
        "ssa_code": ssa_code,
        "converted_code": converted_code,
        "process_tables": split_result["process_tables"],
        "operator_class": operator_class_with_imports,
        "num_args": split_result["num_args"],
        "cuts_used": split_result["cuts_used"],
        "filtered_cuts": split_result["filtered_cuts"],
        "import_statements": import_statements,
        "cleaned_code": cleaned_code,
        "port_assignments": port_assignments or [],
    }
