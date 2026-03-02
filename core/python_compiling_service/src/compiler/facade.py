import copy
from collections import OrderedDict
from typing import Any, Dict, Optional

from .baseline import compile_baseline_mode
from .dependency_graph import VariableDependencyGraph
from .models import CompileRequest, CompileResult
from .orchestrator import compile_non_baseline
from .port_coloring import infer_port_assignments
from .preprocessing import preprocess_code
from .splitter import generate_process_tables_and_split
from .ssa_core import SSA
from .ssa_transform import convert_ssa_to_self
from .type_inference import infer_types_from_code

_CACHE_MAX_SIZE = 128
_COMPILE_CACHE = OrderedDict()
_CACHE_STATS = {"hits": 0, "misses": 0, "evictions": 0}


def compile_udf_legacy(code: str, line_number: Optional[int] = None) -> Dict[str, Any]:
    lines = code.split("\n")
    first_line = lines[0].strip() if lines else ""

    if first_line == "#baseline":
        return compile_baseline_mode(code)

    return compile_non_baseline(
        lines=lines,
        line_number=line_number,
        preprocess_code_fn=preprocess_code,
        infer_types_fn=infer_types_from_code,
        ssa_fn=SSA,
        convert_ssa_fn=convert_ssa_to_self,
        graph_cls=VariableDependencyGraph,
        split_fn=generate_process_tables_and_split,
        infer_port_assignments_fn=infer_port_assignments,
    )


# Backward-compatible hook for existing monkeypatch-based tests.
legacy_compile = compile_udf_legacy


def clear_compile_cache() -> None:
    _COMPILE_CACHE.clear()
    _CACHE_STATS["hits"] = 0
    _CACHE_STATS["misses"] = 0
    _CACHE_STATS["evictions"] = 0


def get_compile_cache_stats() -> dict:
    return {
        "hits": _CACHE_STATS["hits"],
        "misses": _CACHE_STATS["misses"],
        "evictions": _CACHE_STATS["evictions"],
        "size": len(_COMPILE_CACHE),
        "max_size": _CACHE_MAX_SIZE,
    }


def compile_udf(code: str, line_number: Optional[int] = None) -> CompileResult:
    request = CompileRequest(code=code, line_number=line_number)
    cache_key = (request.code, request.line_number)

    if cache_key in _COMPILE_CACHE:
        _CACHE_STATS["hits"] += 1
        cached = _COMPILE_CACHE.pop(cache_key)
        _COMPILE_CACHE[cache_key] = cached  # refresh LRU order
        return CompileResult.from_legacy_dict(copy.deepcopy(cached))

    _CACHE_STATS["misses"] += 1
    legacy_result = legacy_compile(request.code, request.line_number)
    _COMPILE_CACHE[cache_key] = copy.deepcopy(legacy_result)
    if len(_COMPILE_CACHE) > _CACHE_MAX_SIZE:
        _COMPILE_CACHE.popitem(last=False)
        _CACHE_STATS["evictions"] += 1

    return CompileResult.from_legacy_dict(legacy_result)
