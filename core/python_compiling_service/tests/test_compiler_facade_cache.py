import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import src.compiler.facade as facade_module
from src.compiler import clear_compile_cache, compile_udf, get_compile_cache_stats


def test_compile_udf_cache_hit(monkeypatch):
    clear_compile_cache()
    calls = {"count": 0}

    def fake_legacy_compile(code, line_number):
        calls["count"] += 1
        return {
            "ranked_cuts": [],
            "operator_class": f"class Operator: pass  # {code} {line_number}",
            "ssa_code": "ssa",
            "converted_code": "converted",
            "num_args": 1,
            "cuts_used": [],
            "import_statements": [],
            "process_tables": {},
            "filtered_cuts": [],
            "cleaned_code": "cleaned",
        }

    monkeypatch.setattr(facade_module, "legacy_compile", fake_legacy_compile)

    r1 = compile_udf("x=1", None)
    r2 = compile_udf("x=1", None)

    assert calls["count"] == 1
    assert r1.operator_class == r2.operator_class
    stats = get_compile_cache_stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 1
    assert stats["size"] == 1


def test_compile_udf_cache_key_includes_line_number(monkeypatch):
    clear_compile_cache()
    calls = {"count": 0}

    def fake_legacy_compile(code, line_number):
        calls["count"] += 1
        return {
            "ranked_cuts": [],
            "operator_class": f"class Operator: pass  # {line_number}",
            "ssa_code": "ssa",
            "converted_code": "converted",
            "num_args": 1,
            "cuts_used": [],
            "import_statements": [],
            "process_tables": {},
            "filtered_cuts": [],
            "cleaned_code": "cleaned",
        }

    monkeypatch.setattr(facade_module, "legacy_compile", fake_legacy_compile)

    compile_udf("x=1", 3)
    compile_udf("x=1", 4)
    compile_udf("x=1", 3)

    assert calls["count"] == 2
    stats = get_compile_cache_stats()
    assert stats["misses"] == 2
    assert stats["hits"] == 1
    assert stats["size"] == 2
