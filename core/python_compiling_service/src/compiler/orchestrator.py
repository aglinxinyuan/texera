import logging
from typing import Any, Callable, Dict, List, Optional, Type

from .pipeline import (
    apply_type_info_to_graph,
    assemble_compile_result,
    build_ranked_cuts,
    split_imports_and_function_code,
)

logger = logging.getLogger(__name__)


def compile_non_baseline(
    *,
    lines: List[str],
    line_number: Optional[int],
    preprocess_code_fn: Callable[[str], str],
    infer_types_fn: Callable[[str], Dict[str, str]],
    ssa_fn: Callable[[str], str],
    convert_ssa_fn: Callable[[str, str, Optional[Dict[str, str]]], str],
    graph_cls: Type[Any],
    split_fn: Callable[[str, List[dict], str, Optional[Dict[str, str]]], Dict[str, Any]],
    infer_port_assignments_fn: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    import_statements, function_code_str = split_imports_and_function_code(lines)

    cleaned_function_code = preprocess_code_fn(function_code_str)
    logger.debug("Original function code length: %d", len(function_code_str))
    logger.debug("Cleaned function code length: %d", len(cleaned_function_code))

    type_info = infer_types_fn(cleaned_function_code)
    logger.debug("Inferred types: %s", type_info)

    ssa_code = ssa_fn(cleaned_function_code)
    converted_code = convert_ssa_fn(ssa_code, cleaned_function_code, type_info)

    graph = graph_cls(ssa_code)
    apply_type_info_to_graph(graph, type_info)
    ranked_cuts = build_ranked_cuts(graph, line_number)

    split_result = split_fn(converted_code, ranked_cuts, cleaned_function_code, type_info)
    port_assignments: List[Dict[str, Any]] = []
    if infer_port_assignments_fn is not None:
        try:
            port_assignments = infer_port_assignments_fn(cleaned_function_code)
        except Exception as exc:
            logger.debug("Port coloring inference failed: %s", exc)

    return assemble_compile_result(
        ranked_cuts=ranked_cuts,
        ssa_code=ssa_code,
        converted_code=converted_code,
        split_result=split_result,
        import_statements=import_statements,
        cleaned_code=cleaned_function_code,
        port_assignments=port_assignments,
    )
