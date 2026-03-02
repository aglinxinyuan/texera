from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CompileRequest:
    code: str
    line_number: Optional[int] = None


@dataclass(frozen=True)
class CompileResult:
    ranked_cuts: List[Dict[str, Any]]
    operator_class: str
    ssa_code: str
    converted_code: str
    num_args: int
    cuts_used: List[Dict[str, Any]]
    import_statements: List[str]
    process_tables: Dict[str, str]
    filtered_cuts: List[Dict[str, Any]]
    cleaned_code: str
    port_assignments: List[Dict[str, Any]]
    baseline_mode: bool = False

    @staticmethod
    def from_legacy_dict(result: Dict[str, Any]) -> "CompileResult":
        return CompileResult(
            ranked_cuts=result.get("ranked_cuts", []),
            operator_class=result["operator_class"],
            ssa_code=result.get("ssa_code", ""),
            converted_code=result.get("converted_code", ""),
            num_args=result.get("num_args", 0),
            cuts_used=result.get("cuts_used", []),
            import_statements=result.get("import_statements", []),
            process_tables=result.get("process_tables", {}),
            filtered_cuts=result.get("filtered_cuts", []),
            cleaned_code=result.get("cleaned_code", ""),
            port_assignments=result.get("port_assignments", []),
            baseline_mode=result.get("baseline_mode", False),
        )
