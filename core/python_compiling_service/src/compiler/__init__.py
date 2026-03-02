from .baseline import compile_baseline_mode
from .common import get_base_variable_name, infer_line_number_from_code
from .config import DEFAULT_TYPE_SIZE_BYTES, FIRST_USAGE_HEURISTIC_BONUS, MIN_VALID_CUT_LINE
from .cut_strategy import estimate_variable_size, rank_cuts_by_variable_size
from .dependency_graph import DependencyVisitor, VariableDependencyGraph
from .facade import clear_compile_cache, compile_udf, compile_udf_legacy, get_compile_cache_stats
from .graph import (
    are_all_temporal_edges,
    draw_graph_output,
    find_valid_cuts,
    generate_dot_graph,
    get_edges_crossing_line,
    get_topological_order,
    has_cycle,
    visualize_graph,
    visualize_graph_text,
)
from .models import CompileRequest, CompileResult
from .orchestrator import compile_non_baseline
from .pipeline import (
    apply_type_info_to_graph,
    assemble_compile_result,
    build_ranked_cuts,
    maybe_select_ranked_cut,
    prepend_imports_to_operator_class,
    split_imports_and_function_code,
)
from .port_coloring import infer_port_assignments
from .preprocessing import preprocess_code
from .splitter import apply_loop_transformation_to_process_table, generate_process_tables_and_split
from .ssa_core import SSA, SSATransformer
from .ssa_transform import convert_ssa_to_self
from .type_inference import infer_types_from_code
from .use_cases import BASELINE_REFERENCE_UDF, RECOMMENDED_AUTO_CUT_UDF

__all__ = [
    "CompileRequest",
    "CompileResult",
    "DEFAULT_TYPE_SIZE_BYTES",
    "FIRST_USAGE_HEURISTIC_BONUS",
    "MIN_VALID_CUT_LINE",
    "get_base_variable_name",
    "compile_baseline_mode",
    "compile_udf",
    "compile_udf_legacy",
    "clear_compile_cache",
    "get_compile_cache_stats",
    "compile_non_baseline",
    "estimate_variable_size",
    "VariableDependencyGraph",
    "DependencyVisitor",
    "are_all_temporal_edges",
    "draw_graph_output",
    "find_valid_cuts",
    "generate_dot_graph",
    "get_edges_crossing_line",
    "get_topological_order",
    "has_cycle",
    "visualize_graph",
    "visualize_graph_text",
    "infer_line_number_from_code",
    "infer_types_from_code",
    "rank_cuts_by_variable_size",
    "apply_type_info_to_graph",
    "assemble_compile_result",
    "build_ranked_cuts",
    "maybe_select_ranked_cut",
    "prepend_imports_to_operator_class",
    "split_imports_and_function_code",
    "infer_port_assignments",
    "preprocess_code",
    "apply_loop_transformation_to_process_table",
    "generate_process_tables_and_split",
    "SSA",
    "SSATransformer",
    "convert_ssa_to_self",
    "RECOMMENDED_AUTO_CUT_UDF",
    "BASELINE_REFERENCE_UDF",
]
